import logging

from redis.asyncio import Redis

from src.core.config import Settings
from src.utils.cache_service import CacheService

logger = logging.getLogger(__name__)

settings = Settings()

redis: CacheService | None = None


async def get_redis() -> CacheService:
    global redis
    # Проверка, существует ли redis и активно ли соединение
    if not redis or not await redis.redis_client.ping():
        logger.info("Создание клиента Redis...")
        try:
            redis_client = Redis(
                host=settings.REDIS_HOST, port=settings.REDIS_PORT
            )
            if not await redis_client.ping():
                raise ConnectionError("Redis недоступен!")

            redis = CacheService(redis_client)

            logger.info("Клиент Redis успешно создан.")

        except Exception as e:
            logger.error(f"Ошибка при создании клиента Redis: {e}")
            raise

    return redis
