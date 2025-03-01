import logging
from typing import Any, Dict, List
from uuid import UUID

from src.models.models import Genre
from src.services.base_service import BaseService

logger = logging.getLogger(__name__)


class GenreService(BaseService):
    """
    Сервис для работы с жанрами.
    """

    def get_cache_key(self, unique_id: UUID) -> str:
        """
        Генерация ключа для кеша.
        """
        return f"genre:{unique_id}"

    def parse_elastic_response(
            self, response: Dict[str, Any]
    ) -> Genre | None:
        """
        Парсинг ответа от Elasticsearch в объект Genre.
        """
        try:
            source = response["_source"]
            return Genre(
                id=source["uuid"],
                name=source["name"],
                description=source.get("description")
            )
        except KeyError as e:
            logger.error(
                f"Ошибка парсинга данных Elasticsearch: отсутствует ключ {e}"
            )
            return None

    async def get_all_genres(self) -> List[Genre]:
        """
        Получение списка всех жанров.
        """
        return await self.get_all()
