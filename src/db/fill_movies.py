import asyncio
import logging
from decimal import Decimal
from typing import Dict, List

from faker import Faker

from tenacity import (retry, retry_if_exception_type, stop_after_attempt,
                      wait_exponential)

from src.db.elastic import get_elastic
from src.db.redis_client import get_redis
from src.services.add_films import AddFilmService

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Инициализация Faker для генерации фейковых данных
fake = Faker()


# Настройки повторных попыток:
# - Максимум 3 попытки
# - Экспоненциальная задержка между попытками
# - Повторять при любых исключениях
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception_type(Exception),
)
async def generate_fake_film() -> Dict[str, str]:
    return {
        "id": fake.uuid4(),
        "title": fake.sentence(nb_words=3),
        "description": fake.text(max_nb_chars=200),
        "genres": [fake.word() for _ in range(1, 2)],
        "actors": [
            f"{fake.first_name()} {fake.last_name()}" for _ in range(1, 3)
        ],
        "writers": [
            f"{fake.first_name()} {fake.last_name()}" for _ in range(1, 2)
        ],
        "directors": [
            f"{fake.first_name()} {fake.last_name()}" for _ in range(1)
        ],
        "imdb_rating": Decimal(f"{fake.random.uniform(1, 10):.1f}"),
        "release_year": fake.year(),
    }


async def generate_fake_films_async(n: int) -> List[Dict[str, str]]:
    return await asyncio.gather(*(generate_fake_film() for _ in range(n)))


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
)
async def populate_films(
        service: AddFilmService, num_films: int, batch_size: int = 10
) -> None:
    """
    Заполнить Elasticsearch и Redis фейковыми фильмами через AddFilmService.

    :param service: Экземпляр AddFilmService для записи фильмов.
    :param num_films: Общее количество фильмов для генерации.
    :param batch_size: Количество фильмов в одном батче.
    """
    logger.info(
        "Начало заполнения Elasticsearch и Redis. Всего фильмов: %d, "
        "размер батча: %d",
        num_films, batch_size
    )

    for i in range(0, num_films, batch_size):
        batch_start = i + 1
        batch_end = min(i + batch_size, num_films)
        logger.info(
            "Генерация фильмов для батча %d - %d...", batch_start, batch_end
        )

        # Генерация фейковых данных
        films = await generate_fake_films_async(batch_size)

        try:
            # Параллельная обработка фильмов
            await asyncio.gather(
                *[
                    add_film_with_retry(service, film["id"], film)
                    for film in films
                ]
            )
        except Exception:
            logger.exception(
                "Ошибка при добавлении фильмов в Elasticsearch/Redis"
            )
            raise

        logger.info("Батч %d - %d завершён.", batch_start, batch_end)

    logger.info(
        "Все %d фильмов успешно загружены в Elasticsearch и Redis!", num_films
    )


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
)
async def add_film_with_retry(
        service: AddFilmService, film_id: str, film_data: Dict
) -> None:
    """
    Добавить фильм в Elasticsearch и Redis с использованием retry.

    :param service: Экземпляр AddFilmService.
    :param film_id: ID фильма.
    :param film_data: Данные фильма.
    """
    await service.add_film(film_id, film_data)


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=5),
)
async def main() -> None:
    """
    Основная функция для запуска заполнения Elasticsearch и Redis.
    """
    logger.info("Инициализация клиентов Redis и Elasticsearch...")
    redis_client = await get_redis()  # Получаем экземпляр Redis
    logger.info("Получили Redis...")
    elastic_client = await get_elastic()  # Получаем экземпляр Elasticsearch
    logger.info("Получили Elasticsearch...")

    film_service = AddFilmService(elastic=elastic_client)

    # Общее количество фильмов
    num_films = 200
    batch_size = 10

    # Заполняем Elasticsearch и Redis
    try:
        await populate_films(
            film_service, num_films=num_films, batch_size=batch_size
        )
        logger.info("Фильмы успешно добавлены!")
    except Exception as e:
        logger.error("Произошла ошибка при добавлении фильмов: %s", e)
    finally:
        await elastic_client.close()
        logger.info("Соединения с Redis и Elasticsearch закрыты.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logger.critical("Непредвиденная ошибка в программе при добавлении фильмов: %s", e)
