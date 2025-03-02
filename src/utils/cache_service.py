from src.utils.decorators import with_retry
import logging
from redis.asyncio import Redis


logger = logging.getLogger(__name__)


class CacheService:
    def __init__(self, redis_client: Redis, cache_expire: int = 300):
        self.redis_client = redis_client
        self.cache_expire = cache_expire
        logger.info("Инициализация CacheService: cache_expire=%d", self.cache_expire)

    # @with_retry()
    async def get(self, key: str) -> str|None:
        logger.debug("Попытка получить значение из кеша: key=%s", key)
        try:
            value = await self.redis_client.get(key)
            if value is not None:
                logger.info("Ключ найден в кеше: key=%s, value=%s", key, value)
            else:
                logger.info("Ключ отсутствует в кеше: key=%s", key)
            return value
        except Exception as e:
            logger.error("Ошибка при получении значения из кеша: key=%s, error=%s", key, str(e))
            raise

    # @with_retry()
    async def set(self, key: str, value: str) -> None:
        logger.debug("Попытка сохранить значение в кеш: key=%s, value=%s, expire=%d",
                     key, value, self.cache_expire)
        try:
            await self.redis_client.set(key, value, ex=self.cache_expire)
            logger.info("Значение успешно сохранено в кеше: key=%s, expire=%d", key, self.cache_expire)
        except Exception as e:
            logger.error("Ошибка при сохранении значения в кеш: key=%s, value=%s, error=%s",
                         key, value, str(e))
            raise
