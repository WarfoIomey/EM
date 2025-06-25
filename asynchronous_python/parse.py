import os
import asyncio
import aiohttp
import asyncpg
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import Column, Date, DateTime, Integer, String, select
from sqlalchemy.sql import func
from sqlalchemy.orm import declarative_base
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker, 
    create_async_engine
)
from urllib.parse import urljoin
import xlrd
import re

import constants

load_dotenv()

DB_HOST = os.environ.get('DB_HOST', 'localhost')
DB_PORT = os.environ.get('DB_PORT', '5432')
DB_USER = os.environ.get('DB_USER', 'postgres')
DB_PASS = os.environ.get('DB_PASS', '')
DB_NAME = os.environ.get('DB_NAME', 'my_database')
DATABASE_URL = f'postgresql+asyncpg://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

async_engine = create_async_engine(DATABASE_URL)
async_session_maker = async_sessionmaker(async_engine, expire_on_commit=False)
Base = declarative_base()

base_url = "https://spimex.com/markets/oil_products/trades/results/"
all_links = []

files_dir = os.path.join(os.path.dirname(__file__), 'excel_files')


class SpimexTraidingResult(Base):
    '''Колонки таблицы pimex_trading_results.'''

    __tablename__ = 'pimex_trading_results'
    id = Column(Integer, primary_key=True, autoincrement=True)
    exchange_product_id = Column(String, nullable=False)
    exchange_product_name = Column(String, nullable=False)
    oil_id = Column(String(4), nullable=False)
    delivery_basis_id = Column(String(3), nullable=False)
    delivery_basis_name = Column(String, nullable=False)
    delivery_type_id = Column(String(1), nullable=False)
    volume = Column(Integer, nullable=False)
    total = Column(Integer, nullable=False)
    count = Column(Integer, nullable=False)
    date = Column(Date)
    created_on = Column(DateTime, default=func.now())
    updated_on = Column(
        DateTime,
        default=func.now(),
        onupdate=func.now()
    )


async def create_table():
    '''Создание таблицы в БД.'''
    try:
        async with async_engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
    except Exception as e:
        print(f"Ошибка: {e}")


async def create_database():
    '''Создание БД.'''
    conn = await asyncpg.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
        database='postgres'
    )
    try:
        exists = await conn.fetchval(
            "SELECT 1 FROM pg_database WHERE datname = $1", DB_NAME
        )
        if not exists:
            await conn.execute(
                f"CREATE DATABASE {DB_NAME}"
            )
            print(f"База данных {DB_NAME} успешно создана")
        else:
            print(f"База данных {DB_NAME} уже существует")
    except Exception as e:
        print(f'Ошибка при работе с PostgreSQL: {e}')
        raise
    finally:
        if 'conn' in locals():
            await conn.close()


def connection(method):
    '''Декоратор для создания сессии.'''
    async def wrapper(*args, **kwargs):
        async with async_session_maker() as session:
            try:
                result = await method(*args, session=session, **kwargs)
                await session.commit()
                return result
            except Exception as e:
                await session.rollback()
                raise e
            finally:
                await session.close()
    return wrapper


def conn_client(method):
    '''Декоратор для автоматического управления aiohttp-сессией.'''
    async def wrapper(*args, **kwargs):
        timeout = aiohttp.ClientTimeout(total=60)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            try:
                return await method(*args, session=session, **kwargs)
            except Exception as e:
                raise e
            finally:
                await session.close()
    return wrapper


@conn_client
async def get_links(session: aiohttp.ClientSession = None):
    '''Парсинг всех ссылок на файлы с 2023 г.'''
    page_num = 1
    while True:
        url = f"{base_url}?page=page-{page_num}&bxajaxid=d609bce6ada86eff0b6f7e49e6bae904"
        try:
            async with session.get(url) as response:
                html = await response.text()
                pattern = r'href="(/upload/reports/oil_xls/oil_xls_20(2[3-9]|[3-9]\d)\d{10}\.xls\?r=\d+)'
                links = re.findall(pattern, html)
                if not links:
                    break
                full_links = [urljoin(base_url, link[0]) for link in links]
                all_links.extend(full_links)
                page_num += 1
        except aiohttp.ClientError as e:
            print(f"Ошибка при запросе к {url}: {e}")
            break


@conn_client
async def upload_xls(session: aiohttp.ClientSession = None):
    for index, link in enumerate(all_links):
        async with session.get(link) as response:
            with open(f'{files_dir}/{index}.xls', "wb") as f:
                while True:
                    chunk = await response.content.readany()
                    if not chunk:
                        break
                    f.write(chunk)


@connection
async def bulk_save_data(all_data, session: AsyncSession):
    db_objects = [SpimexTraidingResult(**item) for item in all_data]
    session.add_all(db_objects)
    await session.flush()


async def pars_xls(file_name):
    """Асинхронная обёртка для синхронного парсинга XLS"""
    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor() as pool:
        return await loop.run_in_executor(
            pool,
            lambda: parse_xls_sync(file_name)
        )


def parse_xls_sync(file_name):
    """Синхронный парсинг XLS файла"""
    file = os.path.join(files_dir, f"{file_name}")
    wb = xlrd.open_workbook(file)
    sheet = wb.sheet_by_index(0)
    date = datetime.strptime(sheet.cell_value(3, 1)[13:], "%d.%m.%Y").date()
    all_data = []
    row = constants.START_ROW
    if (
        sheet.cell_value(
            constants.UNIT_MEASURE[0],
            constants.UNIT_MEASURE[1]
        ) == 'Единица измерения: Метрическая тонна'
    ):
        while sheet.cell_value(row, constants.FIRST_COLUMN) != 'Итого:':
            product = sheet.cell_value(row, constants.FIRST_COLUMN)
            count_contract = sheet.cell_value(row, constants.COLUMN_CONTRACT)
            if len(product) != 11 or count_contract == '-':
                row += 1
                continue
            data = {
                "exchange_product_id": product,
                "exchange_product_name": sheet.cell_value(
                    row,
                    constants.FIRST_COLUMN+1
                ),
                "oil_id": product[:4],
                "delivery_basis_id": product[4:7],
                "delivery_basis_name": sheet.cell_value(
                    row,
                    constants.FIRST_COLUMN+2
                ),
                "delivery_type_id": product[-1],
                "volume": int(sheet.cell_value(row, constants.FIRST_COLUMN+3)),
                "total": int(sheet.cell_value(row, constants.FIRST_COLUMN+4)),
                "count": int(count_contract),
                "date": date,
            }
            all_data.append(data)
            row += 1
    return all_data


async def process_file(file_num):
    """Обработка одного файла и сохранение в БД"""
    try:
        data = await pars_xls(f'{file_num}.xls')
        if data:
            await bulk_save_data(data)
    except Exception as e:
        print(f'Ошибка обработки файла {file_num}.xls: {str(e)}')


async def main():
    try:
        await create_database()
        await create_table()
        await get_links()
        print(f"Найдено ссылок: {len(all_links)}")
        await upload_xls()
        tasks = [process_file(num) for num in range(len(all_links))]
        await asyncio.gather(*tasks, return_exceptions=True)
    except Exception as e:
        print(f"Ошибка: {e}")
    finally:
        await async_engine.dispose()


if __name__ == '__main__':
    # Асинхронно 43 секунды 100 файлов
    # Синхронно database.task02.parser.py 214 секунды 100 файлов
    t0 = time.time()
    asyncio.run(main())
    print(time.time() - t0)
