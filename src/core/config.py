import os
from typing import Tuple, Set, Dict, Any

from pydantic_settings import BaseSettings
from pydantic import Field

from elasticsearch import (
    BadRequestError as ESBadRequestError,
    ConnectionError as ESConnectionError,
    ConnectionTimeout as ESConnectionTimeout,
    TransportError as ESTransportError
)
from redis.exceptions import (
    ConnectionError as RedisConnectionError,
    RedisError,
    TimeoutError as RedisTimeoutError
)


class Settings(BaseSettings):
    # Общая конфигурация
    PROJECT_NAME: str = Field("movies", env="PROJECT_NAME")

    # Конфигурация Redis
    REDIS_HOST: str = Field("127.0.0.1", env="REDIS_HOST")
    REDIS_PORT: int = Field(6379, env="REDIS_PORT")

    # Конфигурация Elasticsearch
    ELASTIC_HOST: str = Field("elasticsearch", env="ELASTIC_HOST")
    ELASTIC_PORT: int = Field(9200, env="ELASTIC_PORT")

    # Директория проекта
    BASE_DIR: str = Field(
        default_factory=lambda: os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    )

    # Индекс Elasticsearch
    ELASTIC_INDEX: str = "films"

    ELASTIC_EXCEPTIONS: Any = (
        ESConnectionError,
        ESConnectionTimeout,
        ESTransportError,
        ESBadRequestError,
    )

    REDIS_EXCEPTIONS: Any = (
        RedisConnectionError,
        RedisTimeoutError,
        RedisError,
    )

    # Настройки кеширования
    FILM_CACHE_EXPIRE_IN_SECONDS: int = 300

    # Исключения для фильмов
    GET_FILM_BY_ID_EXCLUDE: Dict[str, str] = Field(default_factory=dict)
    GET_FILMS_EXCLUDE: Set[str] = Field(default_factory=lambda: {"description", "genre", "actors", "writers", "directors"})


# Инициализация настроек
settings = Settings()