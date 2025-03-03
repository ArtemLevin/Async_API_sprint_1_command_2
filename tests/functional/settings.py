import logging

from uuid import uuid4

from pydantic import Field
from pydantic_settings import BaseSettings

from tests.functional.es_mapping import ESIndexMapping, get_es_index_mapping

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TestSettings(BaseSettings):
    # Константы для переменных окружения
    ELASTIC_HOST: str = "http://elasticsearch:9200"
    REDIS_HOST: str = "http://redis:6379"
    SERVICE_URL: str = "http://app:8000"

    # Поля настроек
    es_host: str = Field(default=ELASTIC_HOST, json_schema_extra={'env': 'ELASTIC_HOST'})
    es_index: str = "films"
    es_id_field: str = Field(default_factory=lambda: str(uuid4()))
    es_index_mapping: ESIndexMapping = get_es_index_mapping()

    redis_host: str = Field(default=REDIS_HOST, json_schema_extra={'env': 'REDIS_HOST'})
    service_url: str = Field(default=SERVICE_URL, json_schema_extra={'env': 'SERVICE_URL'})


test_settings = TestSettings()
