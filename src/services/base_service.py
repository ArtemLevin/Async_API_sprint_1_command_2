from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from pydantic import BaseModel
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)  # Устанавливаем уровень логирования

class BaseService(ABC):
    def __init__(self, elastic_service: Any, cache_service: Any, index_name: str):
        """
        Базовый сервис для работы с кэшем и Elasticsearch.

        Args:
            elastic_service (Any): Асинхронный клиент Elasticsearch.
            cache_service (Any): Асинхронный клиент кэша.
            index_name (str): Название индекса Elasticsearch.
        """
        self.elastic_service = elastic_service
        self.cache_service = cache_service
        self.index_name = index_name

    @abstractmethod
    def get_cache_key(self, unique_id: Any) -> str:
        """
        Генерация ключа для кэша на основе уникального идентификатора.

        Args:
            unique_id (Any): Уникальный идентификатор.

        Returns:
            str: Ключ для кэша.
        """
        pass

    @abstractmethod
    def parse_elastic_response(self, response: Dict[str, Any]) -> Optional[BaseModel]:
        """
        Парсинг ответа Elasticsearch в объект модели.

        Args:
            response (Dict[str, Any]): Ответ от Elasticsearch.

        Returns:
            Optional[BaseModel]: Распарсенный объект модели.
        """
        pass

    async def get_by_uuid(self, unique_id: Any) -> Optional[BaseModel]:
        """
        Получение объекта по UUID из кэша или Elasticsearch.

        Args:
            unique_id (Any): Уникальный идентификатор объекта.

        Returns:
            Optional[BaseModel]: Объект, соответствующий UUID, или None, если он отсутствует.

        Raises:
            Exception: Если возникает ошибка при запросе к Elasticsearch.
        """
        cache_key = self.get_cache_key(unique_id)

        # Проверка на пустой ключ
        if not cache_key:
            logger.error(f"Пустой ключ кэша для UUID '{unique_id}'")
            return None

        # Попытка получить данные из кэша
        try:
            cached_data = await self.cache_service.get(cache_key)
            if cached_data:
                logger.info(f"Данные для UUID '{unique_id}' найдены в кэше.")
                return self.parse_elastic_response({"_source": cached_data})
        except Exception as e:
            logger.error(f"Ошибка при получении данных из кэша для UUID '{unique_id}': {e}")

        # Если данных в кэше нет, запрос в Elasticsearch
        query = {"query": {"term": {"uuid": str(unique_id)}}}
        try:
            response = await self.elastic_service.search(self.index_name, query)
            hits = self._extract_hits(response)
            if not hits:
                logger.warning(f"Объект с UUID '{unique_id}' не найден в Elasticsearch.")
                return None

            parsed_data = self.parse_elastic_response(hits[0])
            if parsed_data:
                # Сохранение данных в кэш
                try:
                    await self.cache_service.set(cache_key, parsed_data.json())
                    logger.info(f"Данные для UUID '{unique_id}' сохранены в кэше.")
                except Exception as e:
                    logger.error(f"Ошибка при сохранении данных в кэш для UUID '{unique_id}': {e}")

            return parsed_data
        except Exception as e:
            logger.error(f"Ошибка при запросе объекта с UUID '{unique_id}' в Elasticsearch: {e}")
            raise

    async def get_all(self, query: Dict[str, Any] = None, size: int = 1000) -> List[BaseModel]:
        """
        Получение всех объектов из Elasticsearch по произвольному запросу.

        Args:
            query (Dict[str, Any], optional): Запрос к Elasticsearch. По умолчанию возвращаются все документы.
            size (int, optional): Максимальное количество документов для получения. По умолчанию 1000.

        Returns:
            List[BaseModel]: Список объектов.
        """
        query = query or {"query": {"match_all": {}}}
        try:
            response = await self.elastic_service.search(self.index_name, query, size=size)
            hits = self._extract_hits(response)
            return [self.parse_elastic_response(hit) for hit in hits if hit]
        except Exception as e:
            logger.error(f"Ошибка при запросе всех объектов из Elasticsearch: {e}")
            return []

    @staticmethod
    def _extract_hits(response: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Извлечение массива результатов из ответа Elasticsearch.

        Args:
            response (Dict[str, Any]): Ответ от Elasticsearch.

        Returns:
            List[Dict[str, Any]]: Список документов.
        """
        return response.get("hits", {}).get("hits", [])