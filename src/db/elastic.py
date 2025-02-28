import logging
from elasticsearch import AsyncElasticsearch
from elasticsearch import ApiError
from src.utils.elastic_service import ElasticService
from src.core.config import Settings


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

settings = Settings()

es: ElasticService | None = None


async def get_elastic() -> ElasticService:
    global es
    if not es:
        logger.info("Создание клиента Elasticsearch...")
        try:
            es_client = AsyncElasticsearch(
                hosts=[f"http://{settings.ELASTIC_HOST}:{settings.ELASTIC_PORT}"]
            )
            if not await es_client.ping():
                raise ConnectionError("Elasticsearch недоступен")
            es = ElasticService(es_client)
            logger.info("Клиент Elasticsearch успешно создан.")
        except Exception as e:
            logger.error(f"Ошибка при создании клиента Elasticsearch: {e}")
            raise
    return es


async def close_elastic():
    """
    Закрывает соединение с Elasticsearch.
    """
    global es
    if es:
        logger.info("Закрытие соединения с Elasticsearch...")
        await es.close()
        es = None
        logger.info("Соединение с Elasticsearch успешно закрыто.")
    else:
        logger.info("Соединение с Elasticsearch уже было закрыто.")