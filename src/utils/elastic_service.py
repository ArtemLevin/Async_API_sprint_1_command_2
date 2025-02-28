import logging
from elasticsearch import AsyncElasticsearch, ApiError
from src.utils.decorators import with_retry

logger = logging.getLogger(__name__)  # Логгер для текущего модуля


class ElasticService:
    def __init__(self, es_client: AsyncElasticsearch):
        """
        Сервис для работы с Elasticsearch.

        :param es_client: Асинхронный клиент Elasticsearch.
        """
        self.es_client = es_client
        logger.info("Инициализация ElasticService с клиентом Elasticsearch.")

    @with_retry()
    async def search(self, index: str, query: dict) -> dict:
        """
        Выполнить поисковый запрос в Elasticsearch.

        :param index: Название индекса, в котором будет выполнен запрос.
        :param query: Тело запроса (JSON).
        :return: Результат поиска (JSON).
        """
        logger.debug("Попытка выполнить запрос search в Elasticsearch: index=%s, query=%s", index, query)
        try:
            response = await self.es_client.search(index=index, body=query)
            logger.info("Запрос search успешно выполнен: index=%s, hits=%d",
                        index, len(response.get("hits", {}).get("hits", [])))
            return response
        except ApiError as e:
            logger.error("Ошибка при выполнении запроса search в Elasticsearch: index=%s, error=%s",
                         index, str(e))
            raise

    async def index(self, index: str, id: str, body: dict) -> dict:
        """
        Добавить или обновить документ в Elasticsearch.
        """
        logger.debug("Попытка добавить документ в Elasticsearch: index=%s, id=%s", index, id)
        try:
            response = await self.es_client.index(index=index, id=id, body=body)
            logger.info("Документ успешно добавлен: index=%s, id=%s", index, id)
            return response
        except ApiError as e:
            logger.error("Ошибка при добавлении документа в Elasticsearch: index=%s, id=%s, error=%s",
                         index, id, str(e))
            raise

    async def close(self):
        """
        Закрыть соединение с Elasticsearch.

        Этот метод завершает асинхронное соединение с клиентом Elasticsearch,
        чтобы избежать утечек ресурсов.
        """
        logger.info("Закрытие соединения с Elasticsearch.")
        try:
            await self.es_client.close()
            logger.info("Соединение с Elasticsearch успешно закрыто.")
        except Exception as e:
            logger.error("Ошибка при закрытии соединения с Elasticsearch: %s", str(e))
            raise