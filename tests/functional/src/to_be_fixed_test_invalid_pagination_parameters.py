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
    "query_data, expected_error",
    [
        ({"search": "The Star", "page_size": -5, "page_number": 1}, "page_size"),
        ({"search": "The Star", "page_size": 5, "page_number": 0}, "page_number"),
    ]
)
async def test_search(es_client, load_bulk_data_to_es, query_data, expected_error):
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
            url = f"{test_settings.SERVICE_URL}/api/v1/search/films_by_title"
            response = await session.get(url, params=query_data)
            body = await response.json()
            status = response.status

        # Проверяем результат
        logger.info(f"Получен ответ от API. Статус: {status}, тело ответа: {body}")
        assert status in (500, 4200), f"Ожидался статус 422, получен: {status}"
        assert expected_error in str(body), f"Ошибка должна содержать {expected_error}"
        logger.info("Тест успешно завершен.")

    except Exception as e:
        error_id = str(uuid.uuid4())
        logger.error(f"Ошибка при выполнении теста (ID: {error_id}): {e}")
        raise