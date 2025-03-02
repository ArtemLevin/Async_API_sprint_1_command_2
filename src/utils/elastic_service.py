import logging
from typing import Any

from elastic_transport import ObjectApiResponse, TransportError
from elasticsearch import AsyncElasticsearch, ApiError

from src.core.exceptions import ElasticServiceError
from src.utils.decorators import with_retry

logger = logging.getLogger(__name__)


class ElasticService:
    def __init__(self, es_client: AsyncElasticsearch):
        self.es_client = es_client

    # @with_retry()
    async def search(self, index: str, query: dict) -> ObjectApiResponse[Any]:
        logger.debug("Попытка выполнить запрос search в Elasticsearch: index=%s, query=%s", index, query)
        try:
            response = await self.es_client.search(index=index, body=query)
            logger.info("Запрос search успешно выполнен: index=%s, hits=%d",
                        index, len(response.get("hits", {}).get("hits", [])))
            return response
        except (ApiError, ConnectionError, TransportError) as e:
            logger.error("Ошибка при выполнении запроса search в Elasticsearch: index=%s, error=%s",
                         index, str(e))
            raise ElasticServiceError(f"Ошибка при выполнении запроса search: {str(e)}")

    async def index(self, index: str, id: str, body: dict) -> ObjectApiResponse[Any]:
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

    async def index_exists(self, index_name: str) -> bool:
        """
        Проверяет, существует ли индекс в Elasticsearch.
        """
        try:
            return await self.es_client.indices.exists(index=index_name)
        except ApiError as e:
            logger.error("Ошибка при проверке существования индекса %s: %s", index_name, str(e))
            raise

    async def close(self):
        logger.info("Закрытие соединения с Elasticsearch.")
        try:
            await self.es_client.close()
            logger.info("Соединение с Elasticsearch успешно закрыто.")
        except Exception as e:
            logger.error("Ошибка при закрытии соединения с Elasticsearch: %s", str(e))
            raise

    async def is_connected(self) -> bool:
        return await self.es_client.ping()