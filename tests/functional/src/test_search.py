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
async def test_search():
    logger.info("Начало теста: test_search")

    try:
        # 1. Генерируем данные для ES
        logger.info("Генерация данных для Elasticsearch")
        es_data = [{
            'id': str(uuid.uuid4()),
            'genre': [
                {'id': 'action_genre_id', 'name': 'Action'},
                {'id': 'sci_fi_genre_id', 'name': 'Sci-Fi'}
            ],
            'title': 'The Star',
            'description': 'New World',
            'actors': [
                {'id': 'ef86b8ff-3c82-4d31-ad8e-72b69f4e3f95', 'full_name': 'Ann'},
                {'id': 'fb111f22-121e-44a7-b78f-b19191810fbf', 'full_name': 'Bob'}
            ],
            'writers': [
                {'id': 'caf76c67-c0fe-477e-8766-3ab3ff2574b5', 'full_name': 'Ben'},
                {'id': 'b45bd7bc-2e16-46d5-b125-983d356768c6', 'full_name': 'Howard'}
            ],
            'directors': [
                {'id': 'director_id_1', 'full_name': 'Stan'}
            ],
            'imdb_rating': 8.5
        } for _ in range(5)]

        bulk_query: list[dict] = []
        for row in es_data:
            logger.info(f"Создан фильм с ID: {row['id']}")
            data = {'_index': 'films', '_id': row['id']}
            data.update({'_source': row})
            bulk_query.append(data)

        logger.info(f"Подготовлено {len(bulk_query)} записей для загрузки в Elasticsearch")

        # 2. Загружаем данные в ES
        logger.info("Загрузка данных в Elasticsearch")
        es_client = AsyncElasticsearch(hosts=test_settings.es_host, verify_certs=False)

        # Проверяем существование индекса
        if await es_client.indices.exists(index=test_settings.es_index):
            logger.info(f"Индекс '{test_settings.es_index}' уже существует. Удаляем его.")
            await es_client.indices.delete(index=test_settings.es_index)

        logger.info(f"Создание нового индекса: {test_settings.es_index}")
        await es_client.indices.create(index=test_settings.es_index, **test_settings.es_index_mapping)

        # Выполняем массовую загрузку данных
        logger.info("Выполнение массовой загрузки данных в Elasticsearch")
        updated, errors = await async_bulk(client=es_client, actions=bulk_query)

        logger.info(f"Обработано записей: {updated}")
        if errors:
            logger.error("Произошли ошибки при записи данных в Elasticsearch")
            raise Exception('Ошибка записи данных в Elasticsearch')

        await es_client.close()
        logger.info("Данные успешно загружены в Elasticsearch")

        # 3. Запрашиваем данные из ES по API
        logger.info("Отправка запроса к API для поиска данных")
        session = aiohttp.ClientSession()
        url = test_settings.service_url + '/api/v1/search/'
        query_data = {'search': 'The Star'}

        logger.info(f"Запрос к URL: {url}, параметры: {query_data}")
        async with session.get(url, params=query_data) as response:
            body = await response.json()
            headers = response.headers
            status = response.status

        await session.close()

        logger.info(f"Получен ответ от API. Статус: {status}, тело ответа: {body}")

        # 4. Проверяем ответ
        logger.info("Проверка статуса ответа и содержимого")
        assert status == 200, f"Ожидался статус 200, получен: {status}"
        assert len(body) == 50, f"Ожидалось 50 записей, получено: {len(body)}"

        logger.info("Тест успешно завершен.")

    except Exception as e:
        logger.error(f"Ошибка при выполнении теста: {e}")
        raise