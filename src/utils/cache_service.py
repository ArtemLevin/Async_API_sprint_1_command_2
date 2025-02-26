from decorators import with_retry
from redis.asyncio import Redis


class CacheService:
    def __init__(self, redis_client: Redis, cache_expire: int = 300):
        self.redis_client = redis_client
        self.cache_expire = cache_expire

    @with_retry()
    async def get(self, key: str) -> str | None:
        return await self.redis_client.get(key)

    @with_retry()
    async def set(self, key: str, value: str) -> None:
        await self.redis_client.set(key, value, ex=self.cache_expire)
