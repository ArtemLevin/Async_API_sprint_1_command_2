import logging
import uuid

import aiohttp
import pytest_asyncio
import pytest

from elasticsearch import Elasticsearch
from elasticsearch.helpers import async_bulk
from elasticsearch import AsyncElasticsearch

from tests.functional.settings import test_settings


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Фикстура для клиента Elasticsearch
@pytest_asyncio.fixture
async def es_client():
    """
    Создает экземпляр клиента Elasticsearch для тестов.
    После завершения теста удаляет созданный индекс (если он существует) и закрывает соединение.

    :return: Асинхронный клиент Elasticsearch.
    """
    client = AsyncElasticsearch(
        hosts=test_settings.es_host,
        verify_certs=False,
        request_timeout=30
    )
    try:
        yield client  # Возвращаем сам клиент
    finally:
        await client.close()

@pytest.fixture
def generate_film_data():
    """
    Фикстура для генерации данных фильма.
    Может создавать один или несколько фильмов.

    :return: Функция-генератор данных фильма.
    """
    def _generate_film_data(count=1):
        """
        Генерирует указанное количество фильмов.

        :param count: Количество фильмов.
        :return: Список данных фильмов (если count > 1) или один фильм.
        """
        films = []
        for _ in range(count):
            film = {
                'uuid': str(uuid.uuid4()),  # Уникальный идентификатор фильма
                'genre': [
                    {'uuid': str(uuid.uuid4()), 'name': 'Action'},
                    {'uuid': str(uuid.uuid4()), 'name': 'Sci-Fi'}
                ],
                'title': 'The Star',
                'description': 'New World',
                'actors': [
                    {'uuid': 'ef86b8ff-3c82-4d31-ad8e-72b69f4e3f95', 'full_name': 'Ann'},
                    {'uuid': 'fb111f22-121e-44a7-b78f-b19191810fbf', 'full_name': 'Bob'}
                ],
                'writers': [
                    {'uuid': 'caf76c67-c0fe-477e-8766-3ab3ff2574b5', 'full_name': 'Ben'},
                    {'uuid': 'b45bd7bc-2e16-46d5-b125-983d356768c6', 'full_name': 'Howard'}
                ],
                'directors': [
                    {'uuid': str(uuid.uuid4()), 'full_name': 'Stan'}
                ],
                'imdb_rating': 8.5
            }
            films.append(film)
        return films if count > 1 else films[0]

    return _generate_film_data