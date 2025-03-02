
import logging
from typing import List, Optional
from uuid import UUID

from src.models.models import FilmRole, Person
from src.services.base_service import BaseService

logger = logging.getLogger(__name__)


class PersonService(BaseService):
    """
    Сервис для работы с данными о персонах.
    """

    def get_cache_key(self, unique_id: UUID) -> str:
        """
        Генерация ключа для кеша.

        :param unique_id: Уникальный ID персоны (UUID).
        :return: Строка-ключ для кеша.
        """
        return f"person:{unique_id}"

    def parse_elastic_response(self, response: dict) -> Optional[Person]:
        """
        Преобразует ответ Elasticsearch в объект Person.

        :param response: Ответ Elasticsearch (словарь).
        :return: Объект Person или None в случае ошибки.
        """
        try:
            # Извлекаем данные из "_source"
            source = response["_source"]
            print(source)
            # Парсим список фильмов
            films = [
                FilmRole(
                    id=film["id"],  # Предполагается, что "uuid" является строкой
                    roles=film.get("roles", [])
                )
                for film in source.get("films", [])
            ]

            # Создаем объект Person
            return Person(
                id=UUID(source["id"]),  # Преобразуем строку в UUID
                full_name=source["name"],
                films=films
            )

        except KeyError as e:
            logger.error(f"Ошибка парсинга данных Elasticsearch: отсутствует ключ {e}")
            return None
        except ValueError as e:
            logger.error(f"Ошибка преобразования данных: {e}")
            return None

    async def get_all_persons(self) -> List[Person]:
        """
        Получение списка всех персон.

        :return: Список объектов Person.
        """
        try:
            # Получаем сырые данные из Elasticsearch
            raw_persons = await self.elastic_service.search(index="persons", body={"query": {"match_all": {}}})

            # Фильтруем только успешные результаты
            persons = [
                self.parse_elastic_response(hit)
                for hit in raw_persons.get("hits", {}).get("hits", [])
                if self.parse_elastic_response(hit) is not None
            ]

            logger.info(f"Получено {len(persons)} персон.")
            return persons

        except Exception as e:
            logger.error(f"Ошибка при получении данных о персонах: {e}")
            raise
    async def get_person_by_id(self, person_id: UUID) -> Optional[Person]:
        """
        Получение данных о персоне по её уникальному ID.

        :param person_id: Уникальный ID персоны (UUID).
        :return: Объект Person или None, если персона не найдена.
        """
        try:
            # Формируем запрос для поиска по ID
            query = {
                "query": {
                    "term": {
                        "id": str(person_id)  # Преобразуем UUID в строку для запроса
                    }
                }
            }

            # Выполняем запрос к Elasticsearch
            response = await self.elastic_service.search(index="persons", body=query)

            # Проверяем, есть ли результаты
            hits = response.get("hits", {}).get("hits", [])
            if not hits:
                logger.warning(f"Персона с ID {person_id} не найдена.")
                return None

            # Парсим первый результат
            person_data = hits[0]
            return self.parse_elastic_response(person_data)

        except Exception as e:
            logger.error(f"Ошибка при поиске персоны с ID {person_id}: {e}")
            raise