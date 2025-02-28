import logging
from elasticsearch import AsyncElasticsearch
from fastapi import FastAPI
from fastapi.responses import ORJSONResponse
from redis.asyncio import Redis

from api.v1 import films
from api.v1.persons import router as persons_router
from api.v1.genres import router as genres_router
from src.core.config import settings
from db import elastic, redis_client



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
    Событие запуска приложения: инициализация подключения к Redis и Elasticsearch.
    """

    try:
        logger.info("Инициализация подключения к Redis...")
        redis_client.redis = Redis(host=settings.REDIS_HOST, port=settings.REDIS_PORT)
        await redis_client.redis.ping()  # Проверяем доступность Redis
        logger.info("Подключение к Redis успешно установлено.")
    except settings.REDIS_EXCEPTIONS as e:
        logger.error(f"Ошибка подключения к Redis: {e}")
        raise ConnectionError("Не удалось подключиться к Redis. Приложение завершает работу.")

    # Инициализация подключения к Elasticsearch
    try:
        logger.info("Инициализация подключения к Elasticsearch...")
        elastic.es = AsyncElasticsearch(
            hosts=[f'{settings.ELASTIC_HOST}:{settings.ELASTIC_PORT}']
        )
        # Проверяем доступность Elasticsearch
        if not await elastic.es.ping():
            raise ConnectionError("Elasticsearch не отвечает на запросы.")
        logger.info("Подключение к Elasticsearch успешно установлено.")
    except settings.ELASTIC_EXCEPTIONS as e:
        logger.error(f"Ошибка подключения к Elasticsearch: {e}")
        raise ConnectionError("Не удалось подключиться к Elasticsearch. Приложение завершает работу.")

    logger.info("Все подключения успешно установлены.")


@app.on_event('shutdown')
async def shutdown():
    """
    Событие завершения работы приложения: закрытие подключений к Redis и Elasticsearch.
    """
    # Закрытие подключения к Redis
    try:
        if redis_client.redis:
            logger.info("Закрытие подключения к Redis...")
            await redis_client.redis.close()
            logger.info("Подключение к Redis успешно закрыто.")
    except settings.REDIS_EXCEPTIONS as e:
        logger.error(f"Ошибка при закрытии подключения к Redis: {e}")

    # Закрытие подключения к Elasticsearch
    try:
        if elastic.es:
            logger.info("Закрытие подключения к Elasticsearch...")
            await elastic.es.close()
            logger.info("Подключение к Elasticsearch успешно закрыто.")
    except settings.ELASTIC_EXCEPTIONS as e:
        logger.error(f"Ошибка при закрытии подключения к Elasticsearch: {e}")


# Подключение роутеров
app.include_router(films.router, prefix='/api/v1/films', tags=['films'])
app.include_router(persons_router, prefix="/api/v1/persons", tags=["persons"])
app.include_router(genres_router, prefix="/api/v1/genres", tags=["genres"])