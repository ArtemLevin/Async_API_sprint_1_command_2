import logging
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

