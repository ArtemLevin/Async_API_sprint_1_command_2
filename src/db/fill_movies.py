import asyncio
import logging
import random
from decimal import Decimal
from uuid import uuid4

from elasticsearch.helpers import async_bulk
from faker import Faker
from elasticsearch import AsyncElasticsearch

# Импортируем модели
from src.models.models import Film, GenreBase, PersonBase
from src.utils.elastic_service import ElasticService

# Настройка логирования
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Создание экземпляра клиента Elasticsearch
es_client = AsyncElasticsearch(["http://elasticsearch:9200"])

# Создание экземпляра класса ElasticService
elastic_service = ElasticService(es_client)

# Создание экземпляра Faker для генерации фейковых данных
fake = Faker()


async def create_fake_films(num_films: int, index_name: str):
    """
    Создаёт фейковые фильмы и добавляет их в индекс Elasticsearch.
    """
    logger.info(f"Начало создания {num_films} фейковых фильмов.")
    for i in range(num_films):
        # Генерация фейкового фильма
        film = generate_fake_film()
        logger.debug(f"Создан фейковый фильм: {film.title}")

        # Преобразование объекта Film в словарь
        film_dict = film.model_dump(by_alias=True)
        logger.debug(f"Фильм преобразован в словарь: {film_dict}")

        # Добавление фильма в индекс Elasticsearch
        try:
            response = await elastic_service.index(index=index_name, id=film.id, body=film_dict)
            logger.info(f"Фильм '{film.title}' успешно добавлен в индекс {index_name}. Response: {response}")
        except Exception as e:
            logger.error(f"Ошибка при добавлении фильма '{film.title}' в индекс {index_name}: {e}")


def generate_fake_film() -> Film:
    """
    Генерирует фейковый фильм.
    """
    logger.debug("Начало генерации фейкового фильма.")

    # Генерация жанров
    genres = [GenreBase(id=uuid4(), name=fake.word()) for _ in range(random.randint(1, 3))]
    logger.debug(f"Сгенерированы жанры: {[genre.name for genre in genres]}")

    # Генерация актёров
    actors = [PersonBase(id=uuid4(), full_name=fake.name()) for _ in range(random.randint(2, 5))]
    logger.debug(f"Сгенерированы актёры: {[actor.full_name for actor in actors]}")

    # Генерация сценаристов
    writers = [PersonBase(id=uuid4(), full_name=fake.name()) for _ in range(random.randint(1, 3))]
    logger.debug(f"Сгенерированы сценаристы: {[writer.full_name for writer in writers]}")

    # Генерация режиссёров
    directors = [PersonBase(id=uuid4(), full_name=fake.name()) for _ in range(random.randint(1, 2))]
    logger.debug(f"Сгенерированы режиссёры: {[director.full_name for director in directors]}")

    # Создание объекта Film
    film = Film(
        id=uuid4(),
        title=fake.sentence(nb_words=4),
        description=fake.paragraph(nb_sentences=3),
        genre=genres,
        actors=actors,
        writers=writers,
        directors=directors,
        imdb_rating=Decimal(f"{random.uniform(1, 10):.1f}"),
    )
    logger.debug(f"Фильм успешно сгенерирован: {film.title}")
    return film


async def bulk_create_films(films: list[Film], index_name: str):
    """
    Выполняет массовое создание фильмов через Bulk API.
    """
    logger.info(f"Начало массового создания {len(films)} фильмов в индекс {index_name}.")

    actions = [
        {
            "_index": index_name,
            "_id": film.id,
            "_source": film.model_dump(by_alias=True),
        }
        for film in films
    ]
    logger.debug(f"Подготовлено {len(actions)} действий для Bulk API.")

    try:
        await async_bulk(elastic_service.es_client, actions)
        logger.info(f"Успешно добавлено {len(films)} фильмов в индекс {index_name}.")
    except Exception as e:
        logger.error(f"Ошибка при массовом добавлении фильмов в индекс {index_name}: {e}")


async def recreate_index_with_mapping(index_name: str):
    """
    Удаляет существующий индекс (если он существует) и создаёт новый с маппингом.
    """
    logger.info(f"Проверка существования индекса {index_name}...")
    if await elastic_service.index_exists(index_name):
        logger.info(f"Индекс {index_name} уже существует. Удаление индекса...")
        try:
            await es_client.indices.delete(index=index_name, ignore=[400, 404])
            logger.info(f"Индекс {index_name} успешно удалён.")
        except Exception as e:
            logger.error(f"Ошибка при удалении индекса {index_name}: {e}")

    logger.info(f"Создание нового индекса {index_name} с маппингом...")
    mapping = {
        "mappings": {
            "properties": {
                "id": {"type": "keyword"},
                "title": {"type": "text", "analyzer": "standard"},
                "description": {"type": "text", "analyzer": "standard"},
                "genre": {
                    "type": "nested",
                    "properties": {
                        "id": {"type": "keyword"},
                        "name": {"type": "text", "analyzer": "standard"},
                    },
                },
                "actors": {
                    "type": "nested",
                    "properties": {
                        "id": {"type": "keyword"},
                        "full_name": {"type": "text", "analyzer": "standard"},
                    },
                },
                "writers": {
                    "type": "nested",
                    "properties": {
                        "id": {"type": "keyword"},
                        "full_name": {"type": "text", "analyzer": "standard"},
                    },
                },
                "directors": {
                    "type": "nested",
                    "properties": {
                        "id": {"type": "keyword"},
                        "full_name": {"type": "text", "analyzer": "standard"},
                    },
                },
                "imdb_rating": {"type": "float"},
            }
        },
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0,
        },
    }

    try:
        await es_client.indices.create(index=index_name, body=mapping, ignore=400)
        logger.info(f"Индекс {index_name} успешно создан с маппингом.")
    except Exception as e:
        logger.error(f"Ошибка при создании индекса {index_name}: {e}")


async def main():
    """
    Основная функция для создания индекса и заполнения его фейковыми данными.
    """
    index_name = "films"

    logger.info("Начало работы скрипта.")
    try:
        # Пересоздание индекса с маппингом
        await recreate_index_with_mapping(index_name)

        # Создание списка фейковых фильмов
        num_films = 100
        logger.info(f"Генерация {num_films} фейковых фильмов...")
        films = [generate_fake_film() for _ in range(num_films)]
        logger.info(f"Сгенерировано {len(films)} фейковых фильмов.")

        # Массовое добавление фильмов
        logger.info("Начало массового добавления фильмов в Elasticsearch.")
        await bulk_create_films(films, index_name)
        logger.info("Массовое добавление фильмов завершено.")

    except Exception as e:
        logger.error(f"Критическая ошибка во время выполнения скрипта: {e}")
    finally:
        # Закрываем соединение с Elasticsearch
        logger.info("Закрытие соединения с Elasticsearch.")
        await elastic_service.close()
        logger.info("Соединение с Elasticsearch закрыто.")


if __name__ == "__main__":
    asyncio.run(main())