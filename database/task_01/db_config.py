import os
from dotenv import load_dotenv
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import OperationalError

load_dotenv()


class DatabaseConfig:
    """Класс для конфигурации и управления подключением к БД"""

    def __init__(self):
        self.DB_HOST = os.getenv('DB_HOST', 'localhost')
        self.DB_PORT = os.getenv('DB_PORT', '5432')
        self.DB_USER = os.getenv('DB_USER', 'postgres')
        self.DB_PASS = os.getenv('DB_PASS', '')
        self.DB_NAME = os.getenv('DB_NAME', 'my_database')
        self.Base = declarative_base()
        self.engine = None
        self.SessionLocal = None

    def get_db_url(self, dbname=None):
        db = dbname or self.DB_NAME
        """Возвращает URL для подключения к БД"""
        return (
            'postgresql+psycopg2://'
            f'{self.DB_USER}:{self.DB_PASS}@'
            f'{self.DB_HOST}:{self.DB_PORT}/'
            f'{db}'
        )

    def _ensure_database_exists(self):
        """Проверяет и создает БД если она не существует"""
        try:
            conn = psycopg2.connect(
                host=self.DB_HOST,
                port=self.DB_PORT,
                user=self.DB_USER,
                password=self.DB_PASS,
                dbname='postgres'
            )
            conn.autocommit = True
            cursor = conn.cursor()
            cursor.execute(
                f"SELECT 1 FROM pg_database WHERE datname = '{self.DB_NAME}'"
            )
            if not cursor.fetchone():
                cursor.execute(f"CREATE DATABASE {self.DB_NAME}")
                print(f"База данных '{self.DB_NAME}' создана")
            else:
                print(f"База данных '{self.DB_NAME}' уже существует")
        except psycopg2.Error as e:
            print(f"Ошибка работы к PostgreSQL: {e}")
            raise
        finally:
            if 'conn' in locals():
                conn.close()

    def init_db(self):
        """Инициализирует подключение к БД и создаёт таблицы"""
        db_url = self.get_db_url()
        self._ensure_database_exists()
        self.engine = create_engine(db_url)
        self.SessionLocal = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=self.engine
        )
        return self.SessionLocal

    def create_tables(self):
        """Создаёт все таблицы в БД"""
        if not self.engine:
            self.init_db()
        self.Base.metadata.create_all(bind=self.engine)


db_config = DatabaseConfig()
