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

@pytest.mark.asyncio
async def test_search(es_client, generate_film_data):
    """
    Тест проверяет поиск фильмов через Elasticsearch и API.
    """
    logger.info("Начало теста: test_search")

    try:
        # 1. Генерация данных
        logger.info("Генерация данных для Elasticsearch")
        es_data = generate_film_data(count=50)  # Генерируем 50 фильмов

        # 2. Загрузка данных в Elasticsearch
        logger.info("Создание индекса и загрузка данных в Elasticsearch")
        try:
            # Удаляем индекс, если он существует
            if await es_client.indices.exists(index=test_settings.es_index):
                await es_client.indices.delete(index=test_settings.es_index)

            # Создаем индекс с маппингом
            await es_client.indices.create(index=test_settings.es_index, body=test_settings.es_index_mapping)
        except Exception as e:
            logger.error(f"Ошибка при работе с индексом: {e}")
            raise

        # Формируем bulk-запрос
        bulk_query = [
            {
                '_index': test_settings.es_index,
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
            logger.error(f"Не удалось загрузить {len(failed)} записей: {failed}")
            raise Exception("Ошибка записи данных в Elasticsearch")
        logger.info(f"Успешно загружено {success} записей.")

        # 3. Запрос к API
        await asyncio.sleep(3)  # Ждем, пока данные станут доступны для поиска
        async with aiohttp.ClientSession() as session:
            url = f"{test_settings.service_url}/api/v1/search/"
            query_data = {'search': 'The Star', 'page_size': 5, 'page_number': 1}

            logger.info(f"Запрос к URL: {url}, параметры: {query_data}")
            body, headers, status = await fetch_api_response(session, url, query_data)

            # 4. Проверка ответа
            logger.info(f"Получен ответ от API. Статус: {status}, тело ответа: {body}")
            assert status == 200, f"Ожидался статус 200, получен: {status}"
            assert len(body) == 5, f"Ожидалось 5 записей, получено: {len(body)}"

        logger.info("Тест успешно завершен.")

    except Exception as e:
        error_id = str(uuid.uuid4())
        logger.error(f"Ошибка при выполнении теста (ID: {error_id}): {e}")
        raise


async def fetch_api_response(session, url, query_data):
    """
    Выполняет GET-запрос к API.

    :param session: Экземпляр aiohttp. ClientSession.
    :param url: URL для запроса.
    :param query_data: Параметры запроса.
    :return: Тело ответа, заголовки и статус.
    """
    async with session.get(url, params=query_data) as response:
        return await response.json(), response.headers, response.status


