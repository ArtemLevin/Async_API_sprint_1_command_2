from decimal import Decimal
import asyncio
import logging
from typing import List, Dict
import orjson  # Используем orjson вместо json
from faker import Faker
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from redis.exceptions import RedisError
from elasticsearch import ElasticsearchException
from src.db.elastic import get_elastic
from src.db.redis import get_redis
from src.services.film import FilmService

# Настраиваем логгер
logging.basicConfig(level=logging.INFO)  # Устанавливаем уровень логирования
logger = logging.getLogger(__name__)

# Инициализация Faker для генерации фейковых данных
fake = Faker()


@retry(
    stop=stop_after_attempt(3),  # Максимум 3 попытки
    wait=wait_exponential(multiplier=1, min=1, max=10),  # Экспоненциальная задержка между попытками
    retry=retry_if_exception_type(Exception),  # Повторять при любых исключениях
)
async def generate_fake_films(n: int) -> List[Dict[str, str]]:
    """
    Генерация фейковых данных о фильмах.

    :param n: Количество фильмов для генерации.
    :return: Список словарей с данными о фильмах.
    """
    logger.info("Начало генерации %d фейковых фильмов.", n)
    films = []
    for _ in range(n):
        film = {
            "id": fake.uuid4(),  # Уникальный ID фильма
            "title": fake.sentence(nb_words=3),  # Название фильма
            "description": fake.text(max_nb_chars=200),  # Описание фильма
            "genres": [fake.word() for _ in range(2)],  # Список жанров
            "actors": [f"{fake.first_name()} {fake.last_name()}" for _ in range(3)],  # Список актёров
            "writers": [f"{fake.first_name()} {fake.last_name()}" for _ in range(2)],  # Список сценаристов
            "directors": [f"{fake.first_name()} {fake.last_name()}" for _ in range(1)],  # Список режиссёров
            "imdb_rating": Decimal(f"{fake.random.uniform(1, 10):.1f}"),  # Рейтинг фильма (от 1 до 10)
            "release_year": fake.year(),  # Год выпуска фильма
        }
        films.append(film)
    logger.info("Генерация завершена. Сгенерировано %d фильмов.", n)
    return films


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception_type((RedisError, ElasticsearchException)),
)
async def populate_films(service: FilmService, num_films: int, batch_size: int = 1000) -> None:
    """
    Заполнить Elasticsearch и Redis фейковыми фильмами через FilmService.

    :param service: Экземпляр FilmService для записи фильмов.
    :param num_films: Общее количество фильмов для генерации.
    :param batch_size: Количество фильмов в одном батче.
    """
    logger.info("Начало заполнения Elasticsearch и Redis. Всего фильмов: %d, размер батча: %d", num_films, batch_size)

    for i in range(0, num_films, batch_size):
        batch_start = i + 1
        batch_end = min(i + batch_size, num_films)
        logger.info("Генерация фильмов для батча %d - %d...", batch_start, batch_end)

        # Генерация фейковых данных
        films = await generate_fake_films(batch_size)

        try:
            # Параллельная обработка фильмов
            await asyncio.gather(
                *[
                    add_film_with_retry(service, film["id"], film)
                    for film in films
                ]
            )
        except Exception:
            logger.exception("Ошибка при добавлении фильмов в Elasticsearch/Redis")
            raise

        logger.info("Батч %d - %d завершён.", batch_start, batch_end)

    logger.info("Все %d фильмов успешно загружены в Elasticsearch и Redis!", num_films)


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception_type((RedisError, ElasticsearchException)),
)
async def add_film_with_retry(service: FilmService, film_id: str, film_data: Dict) -> None:
    """
    Добавить фильм в Elasticsearch и Redis с использованием retry.

    :param service: Экземпляр FilmService.
    :param film_id: ID фильма.
    :param film_data: Данные фильма.
    """
    await service.add_film(film_id, film_data)


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=5),
    retry=retry_if_exception_type((RedisError, ElasticsearchException)),
)
async def main() -> None:
    """
    Основная функция для запуска заполнения Elasticsearch и Redis.
    """
    logger.info("Инициализация клиентов Redis и Elasticsearch...")
    redis_client = await get_redis()  # Получаем экземпляр Redis
    elastic_client = await get_elastic()  # Получаем экземпляр Elasticsearch

    film_service = FilmService(redis=redis_client, elastic=elastic_client)

    # Общее количество фильмов
    num_films = 200_000
    batch_size = 1000

    # Заполняем Elasticsearch и Redis
    try:
        await populate_films(film_service, num_films=num_films, batch_size=batch_size)
        logger.info("Фильмы успешно добавлены!")
    except Exception as e:
        logger.error("Произошла ошибка при добавлении фильмов: %s", e)
    finally:
        await redis_client.close()
        await elastic_client.close()
        logger.info("Соединения с Redis и Elasticsearch закрыты.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logger.critical("Непредвиденная ошибка в программе: %s", e)