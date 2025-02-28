import logging
from elasticsearch import AsyncElasticsearch
from elasticsearch import ApiError
from src.utils.elastic_service import ElasticService
from src.core.config import Settings


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

settings = Settings()

es: AsyncElasticsearch | None = None


async def get_elastic() -> ElasticService:
    """
    Возвращает экземпляр ElasticService, создавая клиента Elasticsearch при необходимости.
    """
    global es
    if not es:
        logger.info("Создание клиента Elasticsearch...")
        try:
            es = AsyncElasticsearch(
                hosts=[f"http://{settings.ELASTIC_HOST}:{settings.ELASTIC_PORT}"]
            )
            logger.info("Клиент Elasticsearch успешно создан. Выполняем проверку подключения...")
            # Проверка подключения
            if not await es.ping():
                logger.error("Не удалось подключиться к Elasticsearch.")
                raise ConnectionError("Elasticsearch недоступен")
            logger.info("Подключение к Elasticsearch успешно установлено.")
        except ApiError as e:
            logger.error("Ошибка при создании клиента Elasticsearch: %s", str(e))
            raise
        except Exception as e:
            logger.error("Неизвестная ошибка при создании клиента Elasticsearch: %s", str(e))
            raise
    else:
        logger.info("Клиент Elasticsearch уже существует. Возвращаем существующий экземпляр.")
    return ElasticService(es)


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