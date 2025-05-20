import os
from dotenv import load_dotenv
import psycopg2
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, Date
from sqlalchemy.orm import declarative_base, sessionmaker, relationship


load_dotenv()

DB_HOST = os.environ.get('DB_HOST', 'localhost')
DB_PORT = os.environ.get('DB_PORT', '5432')
DB_USER = os.environ.get('DB_USER', 'postgres')
DB_PASS = os.environ.get('DB_PASS', '')
DB_NAME = os.environ.get('DB_NAME', 'my_database')


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
        cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{DB_NAME}'")
        if not cursor.fetchone():
            cursor.execute(f"CREATE DATABASE {DB_NAME}")
            print(f"База данных '{DB_NAME}' создана")
        else:
            print(f"База данных '{DB_NAME}' уже существует")
    except psycopg2.OperationalError as e:
        print(f"Ошибка подключения к PostgreSQL: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()


def main():
    create_database()
    try:
        DATABASE_URL = f'postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
        engine = create_engine(DATABASE_URL)
        Base = declarative_base()

        class Genre(Base):
            __tablename__ = 'genre'
            genre_id = Column(Integer, primary_key=True, autoincrement=True)
            name_genre = Column(String(50), nullable=False)
            books = relationship("Book", back_populates="genre")

        class Author(Base):
            __tablename__ = 'author'
            author_id = Column(Integer, primary_key=True, autoincrement=True)
            name_author = Column(String(50), nullable=False)
            books = relationship("Book", back_populates="author")

        class City(Base):
            __tablename__ = 'city'
            city_id = Column(Integer, primary_key=True, autoincrement=True)
            name_city = Column(String(50), nullable=False)
            days_delivery = Column(Integer)
            clients = relationship('Client', back_populates='city')

        class Book(Base):
            __tablename__ = 'book'
            book_id = Column(Integer, primary_key=True, autoincrement=True)
            title = Column(String(255), nullable=False)
            author_id = Column(Integer, ForeignKey('author.author_id'))
            genre_id = Column(Integer, ForeignKey('genre.genre_id'))
            price = Column(Integer, default=0)
            amount = Column(Integer, default=1)
            author = relationship("Author", back_populates="books")
            genre = relationship("Genre", back_populates="books")
            buy_books = relationship("BuyBook", back_populates="book")

        class Client(Base):
            __tablename__ = 'client'
            client_id = Column(Integer, primary_key=True, autoincrement=True)
            name_client = Column(String(100), nullable=False)
            city_id = Column(Integer, ForeignKey('city.city_id'))
            email = Column(String(255), nullable=False)
            city = relationship('City', back_populates='clients')
            buys = relationship('Buy', back_populates='client')

        class Buy(Base):
            __tablename__ = 'buy'
            buy_id = Column(Integer, primary_key=True, autoincrement=True)
            buy_description = Column(String, nullable=False)
            client_id = Column(Integer, ForeignKey('client.client_id'))
            client = relationship('Client', back_populates='buys')
            buy_books = relationship('BuyBook', back_populates='buy')
            buy_steps = relationship('BuyStep', back_populates='buy')

        class BuyBook(Base):
            __tablename__ = 'buy_book'
            buy_book_id = Column(Integer, primary_key=True, autoincrement=True)
            buy_id = Column(Integer, ForeignKey('buy.buy_id'))
            book_id = Column(Integer, ForeignKey('book.book_id'))
            amount = Column(Integer, nullable=False)
            buy = relationship('Buy', back_populates='buy_books')
            book = relationship('Book', back_populates='buy_books')

        class Step(Base):
            __tablename__ = 'step'
            step_id = Column(Integer, primary_key=True, autoincrement=True)
            name_step = Column(String(255), nullable=False)
            buy_steps = relationship('BuyStep', back_populates='step')

        class BuyStep(Base):
            __tablename__ = 'buy_step'
            buy_step_id = Column(Integer, primary_key=True, autoincrement=True)
            buy_id = Column(Integer, ForeignKey('buy.buy_id'))
            step_id = Column(Integer, ForeignKey('step.step_id'))
            date_step_beg = Column(Date)
            date_step_end = Column(Date)
            buy = relationship('Buy', back_populates='buy_steps')
            step = relationship('Step', back_populates='buy_steps')

        Base.metadata.create_all(engine)
        print("Таблицы созданы успешно")
        Session = sessionmaker(bind=engine)
        with Session() as session:
            new_author = Author(name_author='Кирилл')
            new_genre = Genre(name_genre='Боевик')
            new_city = City(name_city='Иркутск', days_delivery=30)
            session.add_all([new_author, new_genre, new_city])
            session.flush()
            new_book = Book(
                title='История',
                author=new_author,
                genre=new_genre,
                price=500,
                amount=10
            )
            new_client = Client(
                name_client='Борис',
                city=new_city,
                email='boris@gmail.com'
            )
            session.add_all([new_book, new_client])
            session.flush()
            new_buy = Buy(
                buy_description='Покупаю тома истории',
                client=new_client
            )
            session.add(new_buy)
            session.flush()
            new_buy_book = BuyBook(
                buy=new_buy,
                book=new_book,
                amount=2
            )
            new_step = Step(name_step='Ожидание')
            session.add(new_step)
            session.flush()
            new_buy_step = BuyStep(
                buy=new_buy,
                step=new_step,
                date_step_beg='2025-05-19',
                date_step_end='2025-06-19'
            )
            session.add_all([new_buy_book, new_buy_step])
            session.commit()
            print("Данные добавлены добавлен")
            session.close()
    except Exception as e:
        print(f"Ошибка: {e}")


if __name__ == "__main__":
    main()
