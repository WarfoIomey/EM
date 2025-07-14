import os

import asyncpg
from dotenv import load_dotenv
from sqlalchemy.orm import declarative_base
from sqlalchemy.ext.asyncio import (
    async_sessionmaker,
    create_async_engine
)

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
        self.async_engine = None
        self.async_session = None

    def get_db_url(self, dbname=None):
        db = dbname or self.DB_NAME
        """Возвращает URL для подключения к БД"""
        return (
            'postgresql+asyncpg://'
            f'{self.DB_USER}:{self.DB_PASS}@'
            f'{self.DB_HOST}:{self.DB_PORT}/'
            f'{db}'
        )

    async def _ensure_database_exists(self):
        """Проверяет и создает БД если она не существует"""
        try:
            conn = await asyncpg.connect(
                host=self.DB_HOST,
                port=self.DB_PORT,
                user=self.DB_USER,
                password=self.DB_PASS,
                database='postgres'
            )
            try:
                exists = await conn.fetchval(
                    "SELECT 1 FROM pg_database WHERE datname = $1",
                    self.DB_NAME
                )
                if not exists:
                    await conn.execute(
                        f"CREATE DATABASE {self.DB_NAME}"
                    )
                    print(f"База данных {self.DB_NAME} успешно создана")
                else:
                    print(f"База данных '{self.DB_NAME}' уже существует")
            except Exception as e:
                print(f'Ошибка при работе с PostgreSQL: {e}')
                raise
        except Exception as e:
            print(f'Ошибка при подключении к PostgreSQL: {e}')
            raise
        finally:
            if 'conn' in locals():
                await conn.close()

    async def init_db(self):
        """Инициализирует подключение к БД и создаёт таблицы"""
        db_url = self.get_db_url()
        await self._ensure_database_exists()
        self.async_engine = create_async_engine(db_url)
        self.async_session = async_sessionmaker(
            bind=self.async_engine,
            expire_on_commit=False
        )
        return self.async_session

    async def create_tables(self):
        """Создаёт все таблицы в БД"""
        if not self.async_engine:
            await self.init_db()
        try:
            async with self.async_engine.begin() as conn:
                await conn.run_sync(self.Base.metadata.create_all)
        except Exception as e:
            print(f"Ошибка: {e}")


db_config = DatabaseConfig()
