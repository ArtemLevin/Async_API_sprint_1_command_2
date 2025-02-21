from typing import Optional
from redis.asyncio import Redis
from utils.cache_service import CacheService
from core.config import REDIS_HOST, REDIS_PORT

redis: Optional[Redis] = None

async def get_redis() -> CacheService:
    global redis
    if redis:
        await redis.close()  # Закрываем старое соединение
    redis = Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    return CacheService(redis)
