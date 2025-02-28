import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, List

from pydantic import BaseModel, ValidationError
from src.core.exceptions import CacheServiceError, ElasticServiceError

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class BaseService(ABC):
    def __init__(
            self, elastic_service: Any, cache_service: Any, index_name: str
    ):
        self.elastic_service = elastic_service
        self.cache_service = cache_service
        self.index_name = index_name

    @abstractmethod
    def get_cache_key(self, unique_id: Any) -> str:
        pass

    @abstractmethod
    def parse_elastic_response(
            self, response: Dict[str, Any]
    ) -> BaseModel | None:
        pass

    async def get_by_uuid(self, unique_id: Any) -> BaseModel | None:
        """
        Получение объекта по UUID из кэша или Elasticsearch.
        """
        cache_key = self.get_cache_key(unique_id)
        logger.info(f"Начат процесс получения объекта с UUID '{unique_id}'.")

        # Попытка получить данные из кэша
        try:
            cached_data = await self.cache_service.get(cache_key)
            if cached_data:
                logger.info(f"Данные для UUID '{unique_id}' найдены в кэше.")
                try:
                    return self.parse_elastic_response({"_source": cached_data})
                except ValidationError as e:
                    logger.error(
                        f"Ошибка валидации кэшированных данных для UUID "
                        f"'{unique_id}': {e}"
                    )
                    return None
        except Exception as e:
            logger.error(
                f"Ошибка при получении данных из кэша для UUID '{unique_id}': {e}"
            )
            raise CacheServiceError(
                f"Ошибка при обращении к кэшу для UUID '{unique_id}'."
            )

        # Если данных в кэше нет, запрос в Elasticsearch
        logger.info(f"Данные для UUID '{unique_id}' не найдены в кэше. Выполняется запрос в Elasticsearch.")
        query = {"query": {"term": {"uuid": str(unique_id)}}}
        try:
            response = await self.elastic_service.search(index=self.index_name, body=query)
            hits = response.get("hits", {}).get("hits", [])
            if not hits:
                logger.warning(f"Объект с UUID '{unique_id}' не найден в Elasticsearch.")
                return None

            parsed_data = self.parse_elastic_response(hits[0])
            if parsed_data:
                # Сохранение данных в кэш
                try:
                    await self.cache_service.set(cache_key, parsed_data.json())
                    logger.info(f"Данные для UUID '{unique_id}' сохранены в кэш.")
                except Exception as e:
                    logger.error(
                        f"Ошибка при сохранении данных в кэш для UUID '{unique_id}': {e}"
                    )
                    raise CacheServiceError(
                        f"Ошибка при сохранении данных в кэш для UUID '{unique_id}'."
                    )
            return parsed_data
        except Exception as e:
            logger.error(
                f"Ошибка при запросе объекта с UUID '{unique_id}' в Elasticsearch: {e}"
            )
            raise ElasticServiceError(
                f"Ошибка при запросе объекта с UUID '{unique_id}' в Elasticsearch."
            )

    async def get_all(
            self, query: Dict[str, Any] = None, size: int = 1000
    ) -> List[BaseModel]:
        """
        Получение всех объектов из Elasticsearch по произвольному запросу.
        """
        query = query or {"query": {"match_all": {}}}
        logger.info("Начат процесс получения всех объектов.")
        try:
            response = await self.elastic_service.search(index=self.index_name, body=query)
            hits = response.get("hits", {}).get("hits", [])
            logger.info(f"Найдено {len(hits)} объектов в Elasticsearch.")
            return [
                self.parse_elastic_response(hit)
                for hit in hits
                if hit
            ]
        except Exception as e:
            logger.error(f"Ошибка при запросе всех объектов: {e}")
            raise ElasticServiceError("Ошибка при запросе всех объектов.")