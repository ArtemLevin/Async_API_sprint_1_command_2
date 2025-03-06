import asyncio
import logging
import uuid

import aiohttp
import pytest
from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk
from tests.functional.settings import test_settings

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

str_UUID_for_tests = str(uuid.uuid4())

def generate_film_data() -> dict:
    return {
        'uuid': uuid.uuid4(),
        'genre': [
            {'uuid': str_UUID_for_tests, 'name': 'Action'},
            {'uuid': str_UUID_for_tests, 'name': 'Sci-Fi'}
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
            {'uuid': str_UUID_for_tests, 'full_name': 'Stan'}
        ],
        'imdb_rating': 8.5
    }


@pytest.mark.asyncio
async def test_search(es_client):
    logger.info("Начало теста: test_search")

    try:
        # 1. Генерация данных
        logger.info("Генерация данных для Elasticsearch")
        es_data = [generate_film_data() for _ in range(50)]

        # 2. Загрузка данных в Elasticsearch
        try:
            if await es_client.indices.exists(index=test_settings.es_index):
                await es_client.indices.delete(index=test_settings.es_index)
            await es_client.indices.create(index=test_settings.es_index, **test_settings.es_index_mapping)
        except Exception as e:
            logger.error(f"Ошибка при работе с индексом: {e}")
            raise

        bulk_query = [
            {
                '_index': 'films',
                '_id': row['uuid'],
                '_source': row
            }
            for row in es_data
        ]
        success, failed = await async_bulk(client=es_client, actions=bulk_query, refresh='wait_for',  raise_on_error=False, )
        if failed:
            logger.error(f"Не удалось загрузить {len(failed)} записей: {failed}")
            raise Exception("Ошибка записи данных в Elasticsearch")
        logger.info(f"Успешно загружено {success} записей.")

        # 3. Запрос к API
        await asyncio.sleep(3)
        async with aiohttp.ClientSession() as session:
            url = test_settings.service_url + '/api/v1/search/'
            query_data = {'search': 'The Star', 'page_size': 5, 'page_number': 1}

            logger.info(f"Запрос к URL: {url}, параметры: {query_data}")
            body, headers, status = await fetch_api_response(session, url, query_data)

        # 4. Проверка ответа
        logger.info(f"Получен ответ от API. Статус: {status}, тело ответа: {body}")
        assert status == 200, f"Ожидался статус 200, получен: {status}"
        assert len(body) == 5, f"Ожидалось 50 записей, получено: {len(body)}"

        logger.info("Тест успешно завершен.")

    except Exception as e:
        error_id = str_UUID_for_tests
        logger.error(f"Ошибка при выполнении теста (ID: {error_id}): {e}")
        raise


async def fetch_api_response(session, url, query_data):
    async with session.get(url, params=query_data) as response:
        return await response.json(), response.headers, response.status


