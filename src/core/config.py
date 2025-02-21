import os
from logging import config as logging_config
from decouple import config

from core.logger import LOGGING

# Применяем настройки логирования
logging_config.dictConfig(LOGGING)

# Название проекта. Используется в Swagger-документации
PROJECT_NAME = config('PROJECT_NAME', default='movies')

# Настройки Redis
REDIS_HOST = config('REDIS_HOST', default='127.0.0.1')
REDIS_PORT = config('REDIS_PORT', default=6379, cast=int)

# Настройки Elasticsearch
ELASTIC_SCHEMA = config('ELASTIC_SCHEMA', default='http://')
ELASTIC_HOST = config('ELASTIC_HOST', default='127.0.0.1')
ELASTIC_PORT = config('ELASTIC_PORT', default=9200, cast=int)

# Корень проекта
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))