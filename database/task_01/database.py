from db_config import db_config
from models import (
    Author,
    Book,
    Buy,
    BuyBook,
    BuyStep,
    City,
    Client,
    Genre,
    Step
)


def main():
    try:
        SessionLocal = db_config.init_db()
        db_config.create_tables()
        with SessionLocal() as db:
            new_author = Author.create(db, name_author='Кирилл')
            new_genre = Genre.create(db, name_genre='Боевик')
            new_city = City.create(db, name_city='Иркутск', days_delivery=30)
            new_book = Book.create(
                db,
                title='История',
                author=new_author,
                genre=new_genre,
                price=500,
                amount=10
            )
            new_client = Client.create(
                db,
                name_client='Борис',
                city=new_city,
                email='boris@gmail.com'
            )
            new_buy = Buy.create(
                db,
                buy_description='Покупаю тома истории',
                client=new_client
            )
            new_buy_book = BuyBook.create(
                db,
                buy=new_buy,
                book=new_book,
                amount=2
            )
            new_step = Step.create(db, name_step='Ожидание')
            new_buy_step = BuyStep.create(
                db,
                buy=new_buy,
                step=new_step,
                date_step_beg='2025-05-19',
                date_step_end='2025-06-19'
            )
            print("Данные добавлены")
            db.commit()
    except Exception as e:
        print(f"Ошибка инициализации БД: {e}")
        raise


if __name__ == "__main__":
    main()
