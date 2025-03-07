import logging
from typing import List
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

    def parse_elastic_response(self, response: dict) -> Person | None:
        """
        Преобразует ответ Elasticsearch в объект Person.

        :param response: Ответ Elasticsearch (словарь).
        :return: Объект Person или None в случае ошибки.
        """
        try:
            # Извлекаем данные из "_source"
            source = response["_source"]
            # Парсим список фильмов
            films = [
                FilmRole(
                    id=film["uuid"],
                    roles=film.get("roles", [])
                )
                for film in source.get("films", [])
            ]

            # Создаем объект Person
            return Person(
                id=UUID(source["uuid"]),  # Преобразуем строку в UUID
                full_name=source["full_name"],
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

    async def get_person_by_id(self, person_id: UUID) -> Person | None:
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


    async def search_persons_by_full_name(self, query: str, page_size: int = 10, page_number: int = 1) -> List[Person]:
        """
        Поиск фильмов по названию.

        :param query: Строка для поиска (название фильма).
        :param page_size: Количество результатов на странице.
        :param page_number: Номер страницы.
        :return: Список объектов Film.
        """
        try:
            # Формируем запрос для поиска по названию фильма
            search_query = {
                "from": (page_number - 1) * page_size,  # Пагинация: начало выборки
                "size": page_size,                     # Пагинация: размер страницы
                "query": {
                    "match": {
                        "full_name": query                 # Ищем по полю "full_name"
                    }
                }
            }

            logger.info(f"Выполняем поиск фильмов по запросу: {query}")

            # Выполняем запрос к Elasticsearch
            response = await self.elastic_service.search(index="persons", body=search_query)

            # Фильтруем только успешные результаты
            persons = [
                self.parse_elastic_response(hit)
                for hit in response.get("hits", {}).get("hits", [])
                if self.parse_elastic_response(hit) is not None
            ]

            logger.info(f"Найдено {len(persons)} фильмов по запросу '{query}'.")
            return persons

        except Exception as e:
            logger.error(f"Ошибка при поиске фильмов по запросу '{query}': {e}")
            raise