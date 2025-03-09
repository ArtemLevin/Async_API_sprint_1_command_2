import functools
import json
import logging
from typing import Type

from tenacity import (retry, retry_if_exception_type, stop_after_attempt,
                      wait_exponential)

from src.utils.serialization import model_to_dict

logger = logging.getLogger(__name__)


def with_retry(exception: Type[Exception] = Exception):
    """
    Декоратор для повторных попыток выполнения функции с экспоненциальным
    ожиданием.

    :return: Декоратор с логированием для повторных попыток.
    """
    return retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(exception),
        reraise=True,
    )


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
            serialized = json.dumps([model_to_dict(film) for film in result])
            await redis_instance.set(key, serialized)
            log.info("Результаты сохранены в кэше.")
            return result

        return wrapper

    return decorator