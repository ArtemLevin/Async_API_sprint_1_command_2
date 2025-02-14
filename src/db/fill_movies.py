import asyncio
import orjson  # Используем orjson вместо json
from faker import Faker
from elasticsearch import AsyncElasticsearch
from redis.asyncio import Redis
from film_service import FilmService

# Инициализация клиентов Elasticsearch и Redis
elastic = AsyncElasticsearch(hosts=["http://localhost:9200"])
redis = Redis(host="localhost", port=6379, decode_responses=True)

# Инициализация Faker для генерации фейковых данных
fake = Faker()


async def generate_fake_films(n: int):
    """
    Генерация фейковых данных о фильмах.

    :param n: Количество фильмов для генерации.
    :return: Список словарей с данными о фильмах.
    """
    films = []
    for _ in range(n):
        film = {
            "id": fake.uuid4(),  # Уникальный ID фильма
            "title": fake.sentence(nb_words=3),  # Название фильма, например, "Dark Horizon"
            "description": fake.text(max_nb_chars=200),  # Описание фильма
            "genres": [fake.word() for _ in range(2)],  # Список жанров
            "actors": [f"{fake.first_name()} {fake.last_name()}" for _ in range(3)],  # Список актёров
            "rating": round(fake.random.uniform(1, 10), 1),  # Рейтинг фильма (от 1 до 10)
            "release_year": fake.year(),  # Год выпуска фильма
        }
        films.append(film)
    return films


async def populate_films(service: FilmService, num_films: int, batch_size: int = 1000):
    """
    Заполнить Elasticsearch и Redis фейковыми фильмами через FilmService.

    :param service: Экземпляр FilmService для записи фильмов.
    :param num_films: Общее количество фильмов для генерации.
    :param batch_size: Количество фильмов в одном батче.
    """
    for i in range(0, num_films, batch_size):
        print(f"Генерация фильмов {i + 1} - {min(i + batch_size, num_films)}")

        # Генерация фейковых данных
        films = await generate_fake_films(batch_size)

        # Добавляем фильмы через FilmService
        for film in films:
            # Преобразуем данные в JSON с использованием ORJSON
            film_json = orjson.dumps(film).decode("utf-8")  # ORJSON возвращает bytes, поэтому декодируем в строку
            await service.add_film(film_id=film["id"], film_data=orjson.loads(film_json))

        print(f"Батч {i + 1} - {min(i + batch_size, num_films)} завершён.")

    print(f"Все {num_films} фильмов успешно загружены в Elasticsearch и Redis!")


async def main():
    # Инициализация FilmService
    film_service = FilmService(redis=redis, elastic=elastic)

    # Общее количество фильмов
    num_films = 200_000

    # Заполняем Elasticsearch и Redis
    await populate_films(film_service, num_films=num_films, batch_size=1000)


if __name__ == "__main__":
    asyncio.run(main())