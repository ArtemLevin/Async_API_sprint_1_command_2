from redis.asyncio import Redis
from src.utils.cache_service import CacheService
from src.core.config import Settings

settings = Settings()

redis: Redis | None = None


async def get_redis() -> Redis:
    global redis
    if not redis:  # Создаем соединение, если его ещё нет
        redis = Redis(host=settings.REDIS_HOST, port=settings.REDIS_PORT)
    return CacheService(redis)