import uuid
import aiohttp
import pytest
import logging
from tests.functional.settings import test_settings

logger = logging.getLogger(__name__)

@pytest.mark.skipif(test_settings.SKIP == "true", reason="Temporary")
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "query_data",
    [{"search": "John Doe", "page_size": 5, "page_number": 1}]  # Параметры поиска по полному имени персоны
)
async def test_search_person_by_name(es_client, load_bulk_data_to_persons_es, query_data):
    """
    Тест проверяет поиск персон по полному имени через Elasticsearch и API.
    """
    logger.info("Начало теста: test_search_person_by_name")

    try:
        # Массовая загрузка данных в индекс Elasticsearch
        logger.info("Массовая загрузка данных в Elasticsearch")
        es_data = await load_bulk_data_to_persons_es
        logger.info(f"Успешно загружено {len(es_data)} персон в индекс {test_settings.es_persons_index_mapping}.")

        # Создаем HTTP-сессию для запроса к API
        async with aiohttp.ClientSession() as session:
            url = f"{test_settings.SERVICE_URL}/api/v1/persons/persons_by_full_name"
            response = await session.get(url, params=query_data)
            body = await response.json()
            status = response.status

        # Логирование и проверка результата
        logger.info(f"Получен ответ от API. Статус: {status}, тело ответа: {body}")
        assert status == 200, f"Ожидался статус 200, получен: {status}"
        assert len(body) == 5, f"Ожидалось 5 записей, получено: {len(body)}"
        logger.info("Тест успешно завершен.")

    except Exception as e:
        error_id = str(uuid.uuid4())
        logger.error(f"Ошибка при выполнении теста (ID: {error_id}): {e}")
        raise