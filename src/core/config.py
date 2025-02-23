import os
from logging import config as logging_config

from decouple import config
from elasticsearch import BadRequestError as ESBadRequestError
from elasticsearch import ConnectionError as ESConnectionError
from elasticsearch import ConnectionTimeout as ESConnectionTimeout
from elasticsearch import TransportError as ESTransportError
from logger import LOGGING
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import RedisError
from redis.exceptions import TimeoutError as RedisTimeoutError

# Применяем настройки логирования
logging_config.dictConfig(LOGGING)

# Название проекта. Используется в Swagger-документации
PROJECT_NAME = config('PROJECT_NAME', default='movies')

# Настройки Redis
REDIS_HOST = config('REDIS_HOST', default='127.0.0.1')
REDIS_PORT = config('REDIS_PORT', default=6379, cast=int)

# Настройки Elasticsearch
ELASTIC_HOST = config('ELASTIC_HOST', default='127.0.0.1')
ELASTIC_PORT = config('ELASTIC_PORT', default=9200, cast=int)

# Корень проекта
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Имя индекса Elasticsearch
ELASTIC_INDEX = "films"

# Список исключений для Elasticsearch
ELASTIC_EXCEPTIONS = (
    ESConnectionError, ESConnectionTimeout, ESTransportError, ESBadRequestError
)

# Список исключений для Redis
REDIS_EXCEPTIONS = (RedisConnectionError, RedisTimeoutError, RedisError)

# Время жизни кеша в Redis
FILM_CACHE_EXPIRE_IN_SECONDS = 300

# Кортеж с полями, которые нужно исключить из выдачи
# Для полной информации по фильму
GET_FILM_BY_ID_EXCLUDE = {}
# Для поиска, фильтрации и сортировки
GET_FILMS_EXCLUDE = {"description", "genre", "actors", "writers", "directors"}
