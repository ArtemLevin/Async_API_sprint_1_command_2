import json
import functools
import logging

from src.db import redis_client
from src.utils.serialization import model_to_dict

logger = logging.getLogger(__name__)


def redis_cache(key_prefix: str = "cache"):
    """
    Декоратор для кэширования результатов асинхронных методов с использованием Redis.
    """

    def decorator(func):
        @functools.wraps(func)
        async def wrapper(self, *args, **kwargs):
            log = getattr(self, "logger", logger)

            # Определяем клиента Redis: используем self.redis, если он есть, иначе глобальный redis_client
            redis_instance = getattr(self, "cache_service", redis_client)
            if redis_instance is None:
                raise AttributeError("Не найден клиент Redis для кэширования.")

            key = f"{key_prefix}:{func.__name__}:{json.dumps(args, sort_keys=True)}:{json.dumps(kwargs, sort_keys=True)}"
            log.info(f"Проверка кэша по ключу: {key}")

            cached_data = await redis_instance.get(key)
            if cached_data:
                log.info("Данные найдены в кэше, возвращаем результат.")
                return json.loads(cached_data)

            result = await func(self, *args, **kwargs)
            if isinstance(result, list):
                for item in result:
                    serialized = json.dumps(model_to_dict(item))
                    await redis_instance.set(key, serialized)
                    log.info("Результаты сохранены в кэше.")
            else:
                serialized = json.dumps(model_to_dict(result))
                await redis_instance.set(key, serialized)
                log.info("Результаты сохранены в кэше.")
            return result

        return wrapper

    return decorator