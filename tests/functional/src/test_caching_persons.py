import aiohttp
import pytest
import logging
import time

from tests.functional.settings import test_settings


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_search_with_cache(load_bulk_data_to_persons_es):
    """
    Тест проверяет, что при повторном запросе по одному и тому же поисковому запросу
    результат возвращается из кеша Redis.

    """
    query_data = {"search": "John Doe", "page_size": 5, "page_number": 1}

    logger.info("Массовая загрузка данных в Elasticsearch")
    es_data = await load_bulk_data_to_persons_es
    logger.info(f"Успешно загружено {len(es_data)} фильмов в индекс {test_settings.es_persons_index_mapping}.")

    # Создаем HTTP-сессию
    async with aiohttp.ClientSession() as session:
        # Первый запрос (предварительное заполнение кэша)
        start = time.monotonic()
        async with session.get(f"{test_settings.SERVICE_URL}/api/v1/persons/persons_by_full_name", params=query_data) as response:
            body1 = await response.json()
            first_duration = time.monotonic() - start
            assert response.status == 200, f"Первый запрос: ожидался статус 200, получен: {response.status}"

        # Второй запрос (ожидается, что ответ будет возвращён из кеша и время будет меньше)
        start = time.monotonic()
        async with session.get(f"{test_settings.SERVICE_URL}/api/v1/persons/persons_by_full_name", params=query_data) as response:
            body2 = await response.json()
            second_duration = time.monotonic() - start
            assert response.status == 200, f"Второй запрос: ожидался статус 200, получен: {response.status}"

    # Сравниваем длительности запросов; второй запрос должен выполняться быстрее, если результат берётся из кеша.
    logger.info(f"Время первого запроса: {first_duration:.4f} сек, второго запроса: {second_duration:.4f} сек")
    assert second_duration < first_duration, (
        "Время выполнения второго запроса не меньше, чем первого. Проверьте работу кеша Redis."
    )
    # Можно также проверить, что ответы идентичны
    assert body1 == body2, "Ответы первого и второго запроса должны совпадать"