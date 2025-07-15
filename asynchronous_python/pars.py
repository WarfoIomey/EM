import os
import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import aiohttp
import aiofiles
from dotenv import load_dotenv
import time
import xlrd
import re

import constants
from db_config import db_config
from models import SpimexTraidingResult


load_dotenv()


base_url = "https://spimex.com/markets/oil_products/trades/results/"
url_files = "https://spimex.com"

files_dir = os.path.join(os.path.dirname(__file__), 'excel_files')


async def get_links():
    """Генератор для получение ссылок на файлы."""
    timeout = aiohttp.ClientTimeout(total=60)
    page_num = 1
    async with aiohttp.ClientSession(timeout=timeout) as session:
        while True:
            url = f"{base_url}?page=page-{page_num}&bxajaxid=d609bce6ada86eff0b6f7e49e6bae904"
            try:
                async with session.get(url) as response:
                    html = await response.text()
                    pattern = r'href="(/upload/reports/oil_xls/oil_xls_20(2[3-9]|[3-9]\d)\d{10}\.xls\?r=\d+)'
                    links = re.findall(pattern, html)
                    if not links:
                        break
                    for link in links:
                        yield link
                    page_num += 1
            except aiohttp.ClientError as e:
                print(f"Ошибка при запросе к {url}: {e}")
                break


async def upload_xls(link):
    timeout = aiohttp.ClientTimeout(total=60)
    date = datetime.strptime(
        re.search(r"(\d{8})", link[0]).group(1),
        "%Y%m%d"
    ).date()
    filename = os.path.join(files_dir, f"{date}.xls")
    url = url_files + link[0]
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(url) as response:
            async with aiofiles.open(filename, 'wb') as f:
                while True:
                    chunk = await response.content.readany()
                    if not chunk:
                        break
                    await f.write(chunk)
    return filename


async def bulk_save_data(session, file_data):
    if file_data is not None:
        await session.run_sync(
            lambda sync_session: sync_session.add_all(
                file_data
            )
        )
        await session.commit()


async def pars_xls(file_name, pool, loop):
    """Асинхронная обёртка для синхронного парсинга XLS"""
    return await loop.run_in_executor(
        pool,
        lambda: parse_xls_sync(file_name)
    )


def parse_xls_sync(file_name):
    """Синхронный парсинг XLS файла"""
    file = os.path.join(files_dir, f"{file_name}")
    wb = xlrd.open_workbook(file)
    sheet = wb.sheet_by_index(0)
    date_str = sheet.cell_value(3, 1)[13:]
    try:
        date = datetime.strptime(date_str, "%d.%m.%Y").date()
    except ValueError:
        date = None
    model_objects = []
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
            model_object = SpimexTraidingResult(
                exchange_product_id=product,
                exchange_product_name=sheet.cell_value(
                    row,
                    constants.FIRST_COLUMN+1
                ),
                oil_id=product[:4],
                delivery_basis_id=product[4:7],
                delivery_basis_name=sheet.cell_value(
                    row,
                    constants.FIRST_COLUMN+2
                ),
                delivery_type_id=product[-1],
                volume=int(sheet.cell_value(row, constants.FIRST_COLUMN+3)),
                total=int(sheet.cell_value(row, constants.FIRST_COLUMN+4)),
                count=int(count_contract),
                date=date
            )
            model_objects.append(model_object)
            row += 1
        return model_objects


async def process_file(link, session_maker, pool, loop):
    """Скачивание, обработка  и сохранение одного файла в БД"""
    try:
        file_name = await upload_xls(link)
        data = await pars_xls(file_name, pool, loop)
        async with session_maker() as db:
            async with db.begin():
                await bulk_save_data(db, data)
    except Exception as e:
        print(f'Ошибка обработки файла {file_name}: {str(e)}')


async def main():
    try:
        session_maker = await db_config.init_db()
        await db_config.create_tables()
        loop = asyncio.get_running_loop()
        with ThreadPoolExecutor() as pool:
            tasks = []
            async for link in get_links():
                task = asyncio.create_task(
                    process_file(link, session_maker, pool, loop)
                )
                tasks.append(task)
            await asyncio.gather(*tasks)
    except Exception as e:
        print(f"Ошибка: {e}")
    finally:
        await db_config.async_engine.dispose()
        for filename in os.listdir(files_dir):
            if filename.endswith(".xls"):
                file_path = os.path.join(files_dir, filename)
                os.remove(file_path)


if __name__ == '__main__':
    # Асинхронно парсер отработал за 20 сек
    # Синхронно database.task02.parser.py 13 мин
    t0 = time.time()
    asyncio.run(main())
    print(time.time() - t0)
