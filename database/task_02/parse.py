import os
from dotenv import load_dotenv
import psycopg2
from sqlalchemy import create_engine, Column, Date, DateTime, Integer, String
from sqlalchemy.sql import func
from sqlalchemy.orm import declarative_base, sessionmaker
from urllib.request import urlopen, urlretrieve
import xlrd
import re

import constants


load_dotenv()

base_url = "https://spimex.com/markets/oil_products/trades/results/"
url_files = "https://spimex.com"
all_links = []
files_dir = os.path.join(os.path.dirname(__file__), 'excel_files')
DB_HOST = os.environ.get('DB_HOST', 'localhost')
DB_PORT = os.environ.get('DB_PORT', '5432')
DB_USER = os.environ.get('DB_USER', 'postgres')
DB_PASS = os.environ.get('DB_PASS', '')
DB_NAME = os.environ.get('DB_NAME', 'my_database')
DATABASE_URL = f'postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
engine = create_engine(DATABASE_URL)
Base = declarative_base()


class SpimexTraidingResult(Base):
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


def create_database():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASS,
            dbname='postgres'
        )
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(
            f"SELECT 1 FROM pg_database WHERE datname = '{DB_NAME}'"
        )
        if not cursor.fetchone():
            cursor.execute(f"CREATE DATABASE {DB_NAME}")
            print(f'База данных {DB_NAME} создана')
        else:
            print(f'База данных {DB_NAME} уже существует')
    except psycopg2.OperationalError as e:
        print(f'Ошибка подключения к PostgreSQL: {e}')
        raise
    finally:
        if 'conn' in locals():
            conn.close()


def get_links():
    page_num = 1
    while True:
        url = f"{base_url}?page=page-{page_num}&bxajaxid=d609bce6ada86eff0b6f7e49e6bae904"
        try:
            response = urlopen(url)
            html = response.read().decode("utf-8")
            pattern = r'href="(/upload/reports/oil_xls/oil_xls_20(2[3-9]|[3-9]\d)\d{10}\.xls\?r=\d+)'
            links = re.findall(pattern, html)
            if not links:
                break
            all_links.extend(links)
            page_num += 1
        except Exception as e:
            print(f"Ошибка на странице {page_num}:", e)
            break


def upload_xls():
    for index, link in enumerate(all_links):
        filename = os.path.join(files_dir, f"{index}.xls")
        urlretrieve(url_files + link[0], filename)
    print('Все файлы скачаны')


def create_table():
    try:
        engine = create_engine(DATABASE_URL)
        Base.metadata.create_all(engine)
        print("Таблицы созданы успешно")
    except Exception as e:
        print(f"Ошибка: {e}")


def bulk_save_data(data_list):
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        try:
            session.bulk_insert_mappings(
                SpimexTraidingResult,
                data_list
            )
            session.commit()
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()


def pars_xls(file_name):
    file = os.path.join(files_dir, f"{file_name}")
    wb = xlrd.open_workbook(file)
    sheet = wb.sheet_by_index(0)
    date = sheet.cell_value(3, 1)[13:]
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
            if len(product) != 11:
                row += 1
                continue
            count_contract = sheet.cell_value(row, constants.COLUMN_CONTRACT)
            if count_contract == '-':
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
                "volume": sheet.cell_value(row, constants.FIRST_COLUMN+3),
                "total": sheet.cell_value(row, constants.FIRST_COLUMN+4),
                "count": count_contract,
                "date": date,
            }
            all_data.append(data)
            if len(all_data) >= constants.LIMIT_SAVE:
                bulk_save_data(all_data)
                all_data = []
            row += 1
        bulk_save_data(all_data)


def main():
    create_database()
    create_table()
    get_links()
    upload_xls()
    print('Загрузка данных в БД')
    for num in range(len(all_links)):
        pars_xls(f'{num}.xls')
        print(f'Файл {num}.xls обработан')
    print('Данные успешно добавлены в БД')


if __name__ == '__main__':
    main()
