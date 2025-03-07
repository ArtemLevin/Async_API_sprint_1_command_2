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

import pytest
from elasticsearch.helpers import async_bulk

@pytest.fixture
async def load_bulk_data_to_es(es_client, generate_film_data):
    """
    Асинхронная фикстура для массовой загрузки данных фильмов в Elasticsearch.

    :param es_client: Асинхронный клиент Elasticsearch.
    :param generate_film_data: Фикстура для генерации данных фильмов.
    :return: Загруженные данные фильмов.
    """
    index_name = test_settings.es_index
    count = 5  # Можно задать другое значение в тесте, если нужно

    # Генерируем данные фильмов
    es_data = generate_film_data(count=count)

    # Удаляем индекс, если он существует
    if await es_client.indices.exists(index=index_name):
        await es_client.indices.delete(index=index_name)

    # Создаем индекс с маппингом
    await es_client.indices.create(index=index_name, body=test_settings.es_index_mapping)

    # Формируем bulk-запрос
    bulk_query = [
        {
            '_index': index_name,
            '_id': film['uuid'],
            '_source': film
        }
        for film in es_data
    ]

    # Выполняем bulk-запрос
    success, failed = await async_bulk(
        client=es_client,
        actions=bulk_query,
        refresh='wait_for'
    )
    if failed:
        raise Exception(f"Не удалось загрузить {len(failed)} записей: {failed}")

    return es_data

@pytest.fixture
async def fetch_api_response():
    """
    Фикстура для выполнения GET-запросов к API.

    :return: Функция для выполнения GET-запроса.
    """
    async def _fetch(session, url, query_data):
        """
        Выполняет GET-запрос.

        :param session: Экземпляр aiohttp.ClientSession.
        :param url: URL для запроса.
        :param query_data: Параметры запроса.
        :return: Тело ответа, заголовки и статус.
        """
        async with session.get(url, params=query_data) as response:
            return response

    return _fetch


@pytest.fixture
def generate_person_data():
    """
    Фикстура для генерации данных персоны.
    Может создавать одну или несколько персон.

    :return: Функция-генератор данных персоны.
    """
    def _generate_person_data(count=1):
        """
        Генерирует указанное количество персон.

        :param count: Количество персон.
        :return: Список данных персон (если count > 1) или одна персона.
        """
        persons = []
        for _ in range(count):
            person = {
                "uuid": str(uuid.uuid4()),
                "full_name": "John Doe",
                "films": [
                    {
                        "uuid": str(uuid.uuid4()),
                        "roles": ["actor", "director"]
                    },
                    {
                        "uuid": str(uuid.uuid4()),
                        "roles": ["writer"]
                    }
                ]
            }
            persons.append(person)
        return persons if count > 1 else persons[0]

    return _generate_person_data

@pytest.fixture
async def load_bulk_data_to_persons_es(es_client, generate_person_data):
    """
    Асинхронная фикстура для массовой загрузки данных фильмов в Elasticsearch.

    :param es_client: Асинхронный клиент Elasticsearch.
    :param generate_person_data: Фикстура для генерации данных фильмов.
    :return: Загруженные данные фильмов.
    """
    index_name = 'persons'
    count = 5  # Можно задать другое значение в тесте, если нужно

    # Генерируем данные фильмов
    es_data = generate_person_data(count=count)

    # Удаляем индекс, если он существует
    if await es_client.indices.exists(index=index_name):
        await es_client.indices.delete(index=index_name)

    # Создаем индекс с маппингом
    await es_client.indices.create(index=index_name, body=test_settings.es_index_mapping)

    # Формируем bulk-запрос
    bulk_query = [
        {
            '_index': index_name,
            '_id': person['uuid'],
            '_source': person
        }
        for person in es_data
    ]

    # Выполняем bulk-запрос
    success, failed = await async_bulk(
        client=es_client,
        actions=bulk_query,
        refresh='wait_for'
    )
    if failed:
        raise Exception(f"Не удалось загрузить {len(failed)} записей: {failed}")

    return es_data