from sqlalchemy import Column, Date, Integer, String, ForeignKey
from sqlalchemy.orm import relationship

from db_config import db_config


Base = db_config.Base


class BaseModel:
    @classmethod
    def create(cls, session, **kwargs):
        """Базовый метод создания"""
        obj = cls(**kwargs)
        session.add(obj)
        session.flush()
        return obj


class Genre(Base, BaseModel):
    __tablename__ = 'genre'
    genre_id = Column(Integer, primary_key=True, autoincrement=True)
    name_genre = Column(String(50), nullable=False)
    books = relationship("Book", back_populates="genre")


class Author(Base, BaseModel):
    __tablename__ = 'author'
    author_id = Column(Integer, primary_key=True, autoincrement=True)
    name_author = Column(String(50), nullable=False)
    books = relationship("Book", back_populates="author")


class City(Base, BaseModel):
    __tablename__ = 'city'
    city_id = Column(Integer, primary_key=True, autoincrement=True)
    name_city = Column(String(50), nullable=False)
    days_delivery = Column(Integer)
    clients = relationship('Client', back_populates='city')


class Book(Base, BaseModel):
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


class Client(Base, BaseModel):
    __tablename__ = 'client'
    client_id = Column(Integer, primary_key=True, autoincrement=True)
    name_client = Column(String(100), nullable=False)
    city_id = Column(Integer, ForeignKey('city.city_id'))
    email = Column(String(255), nullable=False)
    city = relationship('City', back_populates='clients')
    buys = relationship('Buy', back_populates='client')


class Buy(Base, BaseModel):
    __tablename__ = 'buy'
    buy_id = Column(Integer, primary_key=True, autoincrement=True)
    buy_description = Column(String, nullable=False)
    client_id = Column(Integer, ForeignKey('client.client_id'))
    client = relationship('Client', back_populates='buys')
    buy_books = relationship('BuyBook', back_populates='buy')
    buy_steps = relationship('BuyStep', back_populates='buy')


class BuyBook(Base, BaseModel):
    __tablename__ = 'buy_book'
    buy_book_id = Column(Integer, primary_key=True, autoincrement=True)
    buy_id = Column(Integer, ForeignKey('buy.buy_id'))
    book_id = Column(Integer, ForeignKey('book.book_id'))
    amount = Column(Integer, nullable=False)
    buy = relationship('Buy', back_populates='buy_books')
    book = relationship('Book', back_populates='buy_books')


class Step(Base, BaseModel):
    __tablename__ = 'step'
    step_id = Column(Integer, primary_key=True, autoincrement=True)
    name_step = Column(String(255), nullable=False)
    buy_steps = relationship('BuyStep', back_populates='step')


class BuyStep(Base, BaseModel):
    __tablename__ = 'buy_step'
    buy_step_id = Column(Integer, primary_key=True, autoincrement=True)
    buy_id = Column(Integer, ForeignKey('buy.buy_id'))
    step_id = Column(Integer, ForeignKey('step.step_id'))
    date_step_beg = Column(Date)
    date_step_end = Column(Date)
    buy = relationship('Buy', back_populates='buy_steps')
    step = relationship('Step', back_populates='buy_steps')
