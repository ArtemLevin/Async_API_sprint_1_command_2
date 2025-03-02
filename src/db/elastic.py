import logging

from elasticsearch import ApiError, AsyncElasticsearch, ConnectionError

from src.core.config import Settings
from src.utils.elastic_service import ElasticService

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

settings = Settings()

es: AsyncElasticsearch | None = None


async def get_elastic() -> ElasticService:
    global es
    if not es or not es.ping():  # Проверка, существует ли es и активно ли соединение
        logger.info("Создание клиента Elasticsearch...")
        es_client = None
        try:
            es_client = AsyncElasticsearch(
                hosts=[f"http://{settings.ELASTIC_HOST}:{settings.ELASTIC_PORT}"]
            )
            if not await es_client.ping():
                raise ConnectionError("Elasticsearch недоступен")

            logger.info("Клиент Elasticsearch успешно создан.")
            es = ElasticService(es_client)
        except ConnectionError as ce:
            logger.error(f"Ошибка подключения к Elasticsearch: {ce}")
            if es_client:
                await es_client.close()  # Закрыть соединение в случае ошибки
            raise
        except ApiError as ae:
            logger.error(f"Ошибка API Elasticsearch: {ae}")
            if es_client:
                await es_client.close()  # Закрыть соединение в случае ошибки
            raise
        except Exception as e:
            logger.error(f"Неизвестная ошибка: {e}")
            if es_client:
                await es_client.close()  # Закрыть соединение в случае ошибки
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

