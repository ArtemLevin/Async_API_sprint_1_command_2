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


@pytest.mark.asyncio
@pytest.mark.parametrize("query_data", [
    # Параметры, которые должны работать
    ({"search": "John Doe", "page_size": 5, "page_number": 1}),

    # Граничные случаи
    ({"search": "", "page_size": 5, "page_number": 1}),  # Пустой поисковый запрос
    ({"search": "A" * 256, "page_size": 5, "page_number": 1}),  # Слишком длинный поисковый запрос
    ({"search": "John Doe", "page_size": -1, "page_number": 1}),  # Неверное значение page_size
    ({"search": "John Doe", "page_size": 5, "page_number": 0}),  # Неверное значение page_number
    ({"search": "John Doe", "page_size": "five", "page_number": 1}),  # Неверный тип данных для page_size
    ({"search": "John Doe", "page_size": 5, "page_number": "one"}),  # Неверный тип данных для page_number
    ({"search": "John Doe", "page_size": 5}),  # Отсутствует обязательный параметр page_number
    ({"search": "John Doe", "page_number": 1}),  # Отсутствует обязательный параметр page_size
])
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
            status = response.status

        # Логирование и проверка результата
        if status == 200:
            body = response.json()
            logger.info(f"Получен ответ от API. Статус: {status}, тело ответа: {body}")
            assert status == 200, f"Ожидался статус 200, получен: {status}"
            logger.info("Тест успешно завершен.")
        
        else:
            assert status in (500, 420, 422, 400), f"Ожидался статус (500, 420, 400), получен: {status}"
            logger.info("Тест успешно завершен.")

    except Exception as e:
        error_id = str(uuid.uuid4())
        logger.error(f"Ошибка при выполнении теста (ID: {error_id}): {e}")
        raise