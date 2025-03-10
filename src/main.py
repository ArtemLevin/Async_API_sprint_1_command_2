import logging

from elasticsearch import AsyncElasticsearch
from fastapi import FastAPI
from fastapi.responses import ORJSONResponse
from redis.asyncio import Redis

from src.api.v1 import films, genres, persons
from src.core.config import settings
from src.db.elastic import es, get_elastic
from src.db.redis_client import redis, get_redis
from src.utils.cache_service import CacheService
from src.utils.elastic_service import ElasticService

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

app = FastAPI(
    title=settings.PROJECT_NAME,
    docs_url='/api/openapi',
    openapi_url='/api/openapi.json',
    default_response_class=ORJSONResponse,
)


@app.on_event('startup')
async def startup():
    """
    Событие запуска приложения: инициализация подключения к Redis и
    Elasticsearch.
    """

    try:
        logger.info("Инициализация подключения к Redis...")

        redis = await get_redis()

        # redis = CacheService(Redis(
        #     host=settings.REDIS_HOST, port=settings.REDIS_PORT
        # ))
        # Проверяем доступность Redis
        if not await redis.redis_client.ping():
            raise ConnectionError("Redis не отвечает на запросы.")

        logger.info("Подключение к Redis успешно установлено.")

    except settings.REDIS_EXCEPTIONS as e:
        logger.error(f"Ошибка подключения к Redis: {e}")

        raise ConnectionError(
            "Не удалось подключиться к Redis. Приложение завершает работу."
        )

    # Инициализация подключения к Elasticsearch
    try:
        logger.info("Инициализация подключения к Elasticsearch...")

        es = await get_elastic()
        # es = ElasticService(AsyncElasticsearch(
        #     hosts=[f'http://{settings.ELASTIC_HOST}:{settings.ELASTIC_PORT}']
        # ))
        # Проверяем доступность Elasticsearch
        if not await es.es_client.ping():
            raise ConnectionError("Elasticsearch не отвечает на запросы.")

        logger.info("Подключение к Elasticsearch успешно установлено.")

    except settings.ELASTIC_EXCEPTIONS as e:
        logger.error(f"Ошибка подключения к Elasticsearch: {e}")

        raise ConnectionError(
            "Не удалось подключиться к Elasticsearch. Приложение завершает "
            "работу."
        )

    logger.info("Все подключения успешно установлены.")


@app.on_event('shutdown')
async def shutdown():
    """
    Событие завершения работы приложения: закрытие подключений к Redis и
    Elasticsearch.
    """
    # Закрытие подключения к Redis
    try:
        if redis:
            logger.info("Закрытие подключения к Redis...")

            await redis.close()

            logger.info("Подключение к Redis успешно закрыто.")

    except settings.REDIS_EXCEPTIONS as e:
        logger.error(f"Ошибка при закрытии подключения к Redis: {e}")

    # Закрытие подключения к Elasticsearch
    try:
        if es:
            logger.info("Закрытие подключения к Elasticsearch...")

            await es.close()

            logger.info("Подключение к Elasticsearch успешно закрыто.")

    except settings.ELASTIC_EXCEPTIONS as e:
        logger.error(f"Ошибка при закрытии подключения к Elasticsearch: {e}")


# Подключение роутеров
app.include_router(films.router, prefix='/api/v1/films', tags=['films'])
app.include_router(persons.router, prefix="/api/v1/persons", tags=["persons"])
app.include_router(genres.router, prefix="/api/v1/genres", tags=["genres"])
