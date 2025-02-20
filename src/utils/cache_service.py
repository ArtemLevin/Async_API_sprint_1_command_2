from typing import Optional
from redis.asyncio import Redis
from decorators import with_retry


class CacheService:
    def __init__(self, redis_client: Redis, cache_expire: int = 300):
        self.redis_client = redis_client
        self.cache_expire = cache_expire

    @with_retry()
    async def get(self, key: str) -> Optional[str]:
        return await self.redis_client.get(key)

    @with_retry()
    async def set(self, key: str, value: str) -> None:
        await self.redis_client.set(key, value, ex=self.cache_expire)