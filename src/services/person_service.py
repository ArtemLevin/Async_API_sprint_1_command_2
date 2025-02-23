import logging
from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic import BaseModel

from src.models.models import Film, Person
from src.services.base_service import BaseService

logger = logging.getLogger(__name__)


class PersonService(BaseService):
    """
    Сервис для работы с данными о персонах.
    """

    def get_cache_key(self, unique_id: UUID) -> str:
        return f"person:{unique_id}"

    def parse_elastic_response(
            self, response: Dict[str, Any]
    ) -> Optional[BaseModel]:
        try:
            source = response["_source"]
            films = [
                Film(
                    id=film["uuid"], roles=film.get("roles", [])
                ) for film in source.get("films", [])
            ]
            return Person(
                id=source["uuid"], full_name=source["full_name"], films=films
            )
        except KeyError as e:
            logger.error(
                f"Ошибка парсинга данных Elasticsearch: отсутствует ключ {e}"
            )
            return None

    async def get_all_persons(self) -> List[Person]:
        """
        Получение списка всех персон.
        """
        return await self.get_all()
