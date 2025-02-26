from redis.asyncio import Redis

from src.utils.cache_service import CacheService

redis: Redis | None = None


async def get_redis() -> Redis:
    global redis
    if not redis:  # Создаём соединение, если его ещё нет
        redis = Redis(host="localhost", port=6379)
    return CacheService(redis)
