import uuid
import aiohttp
import pytest
import logging
from tests.functional.settings import test_settings

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

@pytest.mark.skipif(test_settings.SKIP == "true", reason="Temporary")
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

        logger.info("Массовая загрузка данных в Elasticsearch")
        es_data = await load_bulk_data_to_es
        logger.info(f"Успешно загружено {len(es_data)} фильмов в индекс {test_settings.es_index}.")


        async with aiohttp.ClientSession() as session:
            url = f"{test_settings.SERVICE_URL}/api/v1/search/films_by_title"
            response = await session.get(url, params=query_data)
            title_body = await response.json()
            title_status = response.status

        async with aiohttp.ClientSession() as session:
            url = f"{test_settings.SERVICE_URL}/api/v1/search/films_by_description"
            response = await session.get(url, params=query_data)
            description_body = await response.json()
            description_status = response.status

        len_body = 0
        if title_status == 200: len_body += len(title_body)
        if description_status == 200: len_body += len(description_body)

        # Проверяем результат
        assert 200 in (title_status, description_status)
        logger.info(f"Найдено {len_body} записей по фразе 'The Star'")
        logger.info("Тест успешно завершен.")

    except Exception as e:
        error_id = str(uuid.uuid4())
        logger.error(f"Ошибка при выполнении теста (ID: {error_id}): {e}")
        raise
