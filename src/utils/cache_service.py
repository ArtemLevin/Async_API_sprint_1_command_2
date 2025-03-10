import logging

from redis.asyncio import Redis

from src.core.config import settings
from src.core.exceptions import CacheServiceError
from src.utils.decorators import with_retry

logger = logging.getLogger(__name__)


class CacheService:
    def __init__(
        self,
        redis_client: Redis,
        cache_expire: int = settings.CACHE_EXPIRE_IN_SECONDS,
    ):
        self.redis_client = redis_client
        self.cache_expire = cache_expire

    @with_retry(settings.REDIS_EXCEPTIONS)
    async def get(self, key: str, log_info: str = "") -> bytes:
        logger.debug(
            "Попытка получить значение из кеша: key=%s. %s", key, log_info
        )
        try:
            value = await self.redis_client.get(key)

        except settings.REDIS_EXCEPTIONS as e:
            logger.error(
                "Ошибка при получении значения из кеша: key=%s, error=%s. %s",
                key, e, log_info
            )
            raise CacheServiceError(e)

        else:
            if value is not None:
                logger.info(
                    "Ключ найден в кеше: key=%s. %s",
                    key, log_info
                )
                return value

            logger.info("Ключ отсутствует в кеше: key=%s. %s", key, log_info)

            raise CacheServiceError("Ключ отсутствует в кеше: key=%s. %s", key)

    @with_retry(settings.REDIS_EXCEPTIONS)
    async def set(self, key: str, value: bytes, log_info: str = "") -> None:
        logger.debug(
            "Попытка сохранить значение в кеш: "
            "key=%s, value=%s, expire=%d. %s",
            key, value, self.cache_expire, log_info
        )
        try:
            await self.redis_client.set(key, value, ex=self.cache_expire)

        except settings.REDIS_EXCEPTIONS as e:
            logger.error(
                "Ошибка при сохранении значения в кеш:"
                " key=%s, value=%s, error=%s. %s",
                key, value, e, log_info
            )
            raise CacheServiceError(e)

        else:
            logger.info(
                "Значение успешно сохранено в кеше: key=%s, expire=%d. %s",
                key, self.cache_expire, log_info
            )

    async def close(self):
        logger.info("Закрытие соединения с Redis...")

        try:
            await self.redis_client.close()
            logger.info("Соединение с Redis успешно закрыто.")

        except (settings.REDIS_EXCEPTIONS, RuntimeError) as e:
            logger.error(
                "Ошибка при закрытии соединения с Redis: %s", e
            )
            raise CacheServiceError(e)
