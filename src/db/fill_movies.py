import json
import random
import uuid
import psycopg2
import logging
from pydantic import BaseModel, Field
from typing import List
from faker import Faker
from settings import settings  # Импортируем настройки из модуля settings

# Инициализация Faker
fake = Faker()

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# Модели данных
class Genre(BaseModel):
    id: str
    name: str


class Person(BaseModel):
    id: str
    name: str


class Movie(BaseModel):
    id: str
    imdb_rating: float = Field(ge=1.0, le=10.0)  # Рейтинг от 1.0 до 10.0
    genres: List[Genre]
    title: str
    description: str
    directors: List[Person]
    actors: List[Person]
    writers: List[Person]


# Функции для генерации данных
def generate_genres() -> List[dict]:
    """Генерирует список жанров в формате JSON."""
    genres = [
        {"id": str(uuid.uuid4()), "name": "Drama"},
        {"id": str(uuid.uuid4()), "name": "Comedy"},
        {"id": str(uuid.uuid4()), "name": "Action"},
        {"id": str(uuid.uuid4()), "name": "Horror"},
        {"id": str(uuid.uuid4()), "name": "Thriller"},
        {"id": str(uuid.uuid4()), "name": "Sci-Fi"},
    ]
    return random.sample(genres, random.randint(1, 3))


def generate_people(role: str, count: int) -> List[dict]:
    """Генерирует список людей (режиссёров, актёров или сценаристов) в формате JSON."""
    return [{"id": str(uuid.uuid4()), "name": fake.name()} for _ in range(count)]


def generate_movie() -> dict:
    """Генерирует данные одного фильма."""
    movie_data = {
        "id": str(uuid.uuid4()),
        "imdb_rating": round(random.uniform(1.0, 10.0), 1),
        "genres": generate_genres(),
        "title": fake.sentence(nb_words=3),
        "description": fake.text(max_nb_chars=200),
        "directors": generate_people("director", random.randint(1, 2)),
        "actors": generate_people("actor", random.randint(2, 5)),
        "writers": generate_people("writer", random.randint(1, 3)),
    }
    # Валидация
    movie = Movie(**movie_data)
    return movie.model_dump()


# Заполнение базы данных
def insert_movies_to_db(movie_count: int):
    """Вставляет сгенерированные данные фильмов в базу данных."""
    try:
        # Подключение к PostgreSQL
        conn = psycopg2.connect(
            dbname=settings.postgres_name,
            user=settings.postgres_user,
            password=settings.postgres_password,
            host=settings.postgres_host,
            port=settings.postgres_port,
        )
        cursor = conn.cursor()

        # SQL-запрос для вставки данных
        insert_query = """
        INSERT INTO movies (id, imdb_rating, genres, title, description, directors, actors, writers)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        """

        # Генерация и вставка данных
        for _ in range(movie_count):
            movie = generate_movie()
            cursor.execute(
                insert_query,
                (
                    movie["id"],
                    movie["imdb_rating"],
                    json.dumps(movie["genres"]),  # Преобразование списка в JSON
                    movie["title"],
                    movie["description"],
                    json.dumps(movie["directors"]),  # Преобразование списка в JSON
                    json.dumps(movie["actors"]),  # Преобразование списка в JSON
                    json.dumps(movie["writers"]),  # Преобразование списка в JSON
                ),
            )
        conn.commit()
        logger.info(f"{movie_count} фильмов успешно добавлено в базу данных.")
    except Exception as e:
        logger.error(f"Ошибка при вставке данных: {e}")
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    # Укажите количество фильмов для генерации
    NUMBER_OF_MOVIES = 50
    logger.info(f"Начало генерации и записи {NUMBER_OF_MOVIES} фильмов в базу данных.")
    insert_movies_to_db(NUMBER_OF_MOVIES)
    logger.info("Процесс завершен.")