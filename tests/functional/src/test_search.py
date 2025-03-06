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

# @pytest.mark.asyncio
# async def test_search(es_client, load_bulk_data_to_es, fetch_api_response):
#     """
#     Тест проверяет поиск фильмов через Elasticsearch и API.
#     """
#     logger.info("Начало теста: test_search")
# 
#     try:
#         # 1. Массовая загрузка данных (50 фильмов) в Elasticsearch
#         logger.info("Массовая загрузка данных в Elasticsearch")
#         index_name = test_settings.es_index
#         es_data = await load_bulk_data_to_es
#         logger.info(f"Успешно загружено {len(es_data)} фильмов в индекс {index_name}.")
# 
#         # 2. Запрос к API
#         await asyncio.sleep(3)  # Ждем, пока данные станут доступны для поиска
#         async with aiohttp.ClientSession() as session:
#             url = f"{test_settings.service_url}/api/v1/search/"
#             query_data = {'search': 'The Star', 'page_size': 5, 'page_number': 1}
# 
#             logger.info(f"Запрос к URL: {url}, параметры: {query_data}")
#             body, headers, status = await fetch_api_response(session, url, query_data)
# 
#             # 3. Проверка ответа
#             logger.info(f"Получен ответ от API. Статус: {status}, тело ответа: {body}")
#             assert status == 200, f"Ожидался статус 200, получен: {status}"
#             assert len(body) == 5, f"Ожидалось 5 записей, получено: {len(body)}"
# 
#         logger.info("Тест успешно завершен.")
# 
#     except Exception as e:
#         error_id = str(uuid.uuid4())
#         logger.error(f"Ошибка при выполнении теста (ID: {error_id}): {e}")
#         raise


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "query_data",
    [{"search": "The Star", "page_size": 5, "page_number": 1}]  # Параметры поиска
)
async def test_search(es_client, load_bulk_data_to_es, query_data):
    """
    Тест проверяет поиск фильмов через Elasticsearch и API.
    """
    logger.info("Начало теста: test_search")

    try:
        # Массовая загрузка данных
        logger.info("Массовая загрузка данных в Elasticsearch")
        es_data = await load_bulk_data_to_es
        logger.info(f"Успешно загружено {len(es_data)} фильмов в индекс {test_settings.es_index}.")

        # Создаем HTTP-сессию
        async with aiohttp.ClientSession() as session:
            url = f"{test_settings.SERVICE_URL}/api/v1/search/"
            response = await session.get(url, params=query_data)
            body = await response.json()
            status = response.status

        # Проверяем результат
        logger.info(f"Получен ответ от API. Статус: {status}, тело ответа: {body}")
        assert status == 200, f"Ожидался статус 200, получен: {status}"
        assert len(body) == 5, f"Ожидалось 5 записей, получено: {len(body)}"
        logger.info("Тест успешно завершен.")

    except Exception as e:
        error_id = str(uuid.uuid4())
        logger.error(f"Ошибка при выполнении теста (ID: {error_id}): {e}")
        raise
