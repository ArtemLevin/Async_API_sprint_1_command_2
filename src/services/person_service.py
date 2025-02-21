import logging
from typing import Any, Dict, Optional, List
from uuid import UUID

from pydantic import BaseModel

from models.models import Film, Person
from services.base_service import BaseService

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)  # Устанавливаем уровень логирования


class PersonService(BaseService):
    """
    Сервис для работы с данными о персонажах и актерах.
    """

    def get_cache_key(self, unique_id: UUID) -> str:
        """
        Генерация ключа для кэша на основе UUID персоны.

        Args:
            unique_id (UUID): Уникальный идентификатор персоны.

        Returns:
            str: Ключ в формате 'person:{UUID}'.
        """
        if not unique_id:
            logger.error("UUID не может быть пустым при генерации ключа кэша.")
            raise ValueError("UUID не может быть пустым.")
        return f"person:{unique_id}"

    def parse_elastic_response(self, response: Dict[str, Any]) -> Optional[BaseModel]:
        """
        Парсинг данных из ответа Elasticsearch в объект модели `Person`.

        Args:
            response (Dict[str, Any]): Ответ Elasticsearch, содержащий данные о персоне.

        Returns:
            Optional[BaseModel]: Объект `Person` или None, если парсинг не удался.
        """
        try:
            source = response["_source"]
            films = [
                Film(
                    uuid=film["uuid"],
                    roles=film.get("roles", [])  # Роли могут отсутствовать, используем пустой список по умолчанию
                )
                for film in source.get("films", [])  # Если у персоны нет фильмов, обрабатываем как пустой список
            ]
            return Person(
                uuid=source["uuid"],
                full_name=source["full_name"],
                films=films
            )
        except KeyError as e:
            logger.error(f"Ошибка парсинга данных Elasticsearch: отсутствует ключ {e}. Ответ: {response}")
            return None
        except Exception as e:
            logger.error(f"Неизвестная ошибка при парсинге данных Elasticsearch: {e}. Ответ: {response}")
            return None

    async def get_all_persons(self) -> List[Person]:
        """
        Получение списка всех персон из Elasticsearch.

        Returns:
            List[Person]: Список объектов `Person`.
        """
        try:
            persons = await self.get_all()  # Используем метод из базового сервиса
            logger.info(f"Получено {len(persons)} персон из Elasticsearch.")
            return persons
        except Exception as e:
            logger.error(f"Ошибка при получении всех персон: {e}")
            return []

    async def get_person_by_uuid(self, unique_id: UUID) -> Optional[Person]:
        """
        Получение информации о персоне по её UUID.

        Args:
            unique_id (UUID): Уникальный идентификатор персоны.

        Returns:
            Optional[Person]: Объект `Person` или None, если персона не найдена.
        """
        try:
            person = await self.get_by_uuid(unique_id)
            if person:
                logger.info(f"Персона с UUID '{unique_id}' успешно найдена.")
            else:
                logger.warning(f"Персона с UUID '{unique_id}' не найдена.")
            return person
        except Exception as e:
            logger.error(f"Ошибка при получении персоны с UUID '{unique_id}': {e}")
            return None