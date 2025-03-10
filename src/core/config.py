import os
from typing import Any, ClassVar

from elastic_transport import TransportError as ESTransportError
from elasticsearch import ApiError as ESApiError
from pydantic import Field
from pydantic_settings import BaseSettings
from redis.exceptions import RedisError


class Settings(BaseSettings):
    # Общая конфигурация
    PROJECT_NAME: str = Field("movies", env="PROJECT_NAME")

    # Конфигурация Redis
    REDIS_HOST: str = Field("redis", env="REDIS_HOST")
    REDIS_PORT: int = Field(6379, env="REDIS_PORT")

    # Конфигурация Elasticsearch
    ELASTIC_HOST: str = Field("elasticsearch", env="ELASTIC_HOST")
    ELASTIC_PORT: int = Field(9200, env="ELASTIC_PORT")

    # Директория проекта
    BASE_DIR: str = Field(
        default_factory=lambda: os.path.dirname(
            os.path.dirname(os.path.abspath(__file__))
        )
    )

    ELASTIC_EXCEPTIONS: Any = (ESApiError, ESTransportError)

    REDIS_EXCEPTIONS: Any = (RedisError,)

    ELASTIC_RESPONSE_SIZE: int = 1000

    # Настройки кеширования
    CACHE_EXPIRE_IN_SECONDS: int = 300

    NOT_FOUND: ClassVar[bytes] = b'"not_found"'


# Инициализация настроек
settings = Settings()
