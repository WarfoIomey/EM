import os
from datetime import datetime
import time

from urllib.request import urlopen, urlretrieve
from urllib.error import URLError, HTTPError
import xlrd
import re

import constants
from models import SpimexTraidingResult
from database.task_01.db_config import db_config


base_url = "https://spimex.com/markets/oil_products/trades/results/"
url_files = "https://spimex.com"
files_dir = os.path.join(os.path.dirname(__file__), 'excel_files')


def get_links():
    page_num: int = 1
    while True:
        url = f"{base_url}?page=page-{page_num}&bxajaxid=d609bce6ada86eff0b6f7e49e6bae904"
        response = urlopen(url)
        html = response.read().decode("utf-8")
        pattern = r'href="(/upload/reports/oil_xls/oil_xls_20(2[3-9]|[3-9]\d)\d{10}\.xls\?r=\d+)'
        links = re.findall(pattern, html)
        if not links:
            break
        yield from links
        page_num += 1


def upload_xls(link: str):
    date = datetime.strptime(
        re.search(r"(\d{8})", link[0]).group(1),
        "%Y%m%d"
    ).date()
    filename = os.path.join(files_dir, f"{date}.xls")
    try:
        urlretrieve(url_files + link[0], filename)
        print(f'Файл {link[0]} скачан')
        return date
    except HTTPError as e:
        print(f"Ошибка HTTP {e.code} при скачивании {link[0]}")
    except URLError as e:
        print(f"Ошибка URL: {e.reason}")
    except Exception as e:
        print(f"Неизвестная ошибка: {str(e)}")


def bulk_save_data(db, file_data: list):
    if file_data is not None:
        try:
            db.add_all(file_data)
            db.commit()
        except Exception as e:
            raise e


def pars_xls(file_name) -> list:
    file = os.path.join(files_dir, f"{file_name}")
    wb = xlrd.open_workbook(file)
    sheet = wb.sheet_by_index(0)
    date_str: str = sheet.cell_value(3, 1)[13:]
    try:
        date = datetime.strptime(date_str, "%d.%m.%Y").date()
    except ValueError:
        date = None
    model_objects: list = []
    row: int = constants.START_ROW
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
                volume=sheet.cell_value(row, constants.FIRST_COLUMN+3),
                total=sheet.cell_value(row, constants.FIRST_COLUMN+4),
                count=count_contract,
                date=date
            )
            model_objects.append(model_object)
            row += 1
        return model_objects


def main():
    SessionLocal = db_config.init_db()
    db_config.create_tables()
    with SessionLocal() as db:
        for link in get_links():
            file_name = upload_xls(link)
            data: list = pars_xls(f'{file_name}.xls')
            bulk_save_data(db, data)
            print(f'Файл {file_name}.xls обработан')
    for filename in os.listdir(files_dir):
        if filename.endswith(".xls"):
            file_path = os.path.join(files_dir, filename)
            os.remove(file_path)


if __name__ == '__main__':
    t0 = time.time()
    main()
    print(time.time() - t0)
