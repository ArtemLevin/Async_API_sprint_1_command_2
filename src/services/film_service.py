import logging
from decimal import Decimal
from typing import List
from uuid import UUID

from src.models.models import Film, GenreBase, PersonBase
from src.services.base_service import BaseService

logger = logging.getLogger(__name__)


class FilmService(BaseService):
    """
    Сервис для работы с данными о фильмах.
    """

    def get_cache_key(self, unique_id: UUID) -> str:
        """
        Генерация ключа для кеша.

        :param unique_id: Уникальный ID фильма (UUID).
        :return: Строка-ключ для кеша.
        """
        return f"film:{unique_id}"

    def parse_elastic_response(self, response: dict) -> Film | None:
        """
        Преобразует ответ Elasticsearch в объект Film.

        :param response: Ответ Elasticsearch (словарь).
        :return: Объект Film или None в случае ошибки.
        """
        try:
            # Извлекаем данные из "_source"
            source = response["_source"]

            # Создаем объект Film
            return Film(
                id=UUID(source["uuid"]),
                title=source["title"],
                description=source.get("description", ""),
                imdb_rating=Decimal(source.get("imdb_rating", "0.0")),
                genre=[GenreBase(id=genre["uuid"], name=genre["name"]) for genre in source.get("genre", [])],
                actors=[
                    PersonBase(id=actor["uuid"], full_name=actor["full_name"])
                    for actor in source.get("actors", [])
                ],
                writers=[
                    PersonBase(id=writer["uuid"], full_name=writer["full_name"])
                    for writer in source.get("writers", [])
                ],
                directors=[
                    PersonBase(id=director["uuid"], full_name=director["full_name"])
                    for director in source.get("directors", [])
                ]
            )

        except KeyError as e:
            logger.error(f"Ошибка парсинга данных Elasticsearch: отсутствует ключ {e}")
            return None
        except ValueError as e:
            logger.error(f"Ошибка преобразования данных: {e}")
            return None

    async def get_all_films(self) -> List[Film]:
        """
        Получение списка всех фильмов.

        :return: Список объектов Film.
        """
        try:
            # Получаем сырые данные из Elasticsearch
            raw_films = await self.elastic_service.search(index="films", body={"query": {"match_all": {}}})

            # Фильтруем только успешные результаты
            films = [
                self.parse_elastic_response(hit)
                for hit in raw_films.get("hits", {}).get("hits", [])
                if self.parse_elastic_response(hit) is not None
            ]

            logger.info(f"Получено {len(films)} фильмов.")
            return films

        except Exception as e:
            logger.error(f"Ошибка при получении данных о фильмах: {e}")
            raise

    async def get_film_by_id(self, film_id: UUID) -> Film | None:
        """
        Получение данных о фильме по его уникальному ID.

        :param film_id: Уникальный ID фильма (UUID).
        :return: Объект Film или None, если фильм не найден.
        """
        try:
            # Формируем запрос для поиска по ID
            query = {
                "query": {
                    "term": {
                        "uuid.keyword": str(film_id)  # Преобразуем UUID в строку для запроса
                    }
                }
            }
            logger.warning(f"Выполняем запрос к Elasticsearch from get film by id")
            # Выполняем запрос к Elasticsearch
            response = await self.elastic_service.search(index="films", body=query)

            # Проверяем, есть ли результаты
            hits = response.get("hits", {}).get("hits", [])
            if not hits:
                logger.warning(f"Фильм с ID {film_id} не найден.")
                return None

            # Парсим первый результат
            film_data = hits[0]
            return self.parse_elastic_response(film_data)

        except Exception as e:
            logger.error(f"Ошибка при поиске фильма с ID {film_id}: {e}")
            raise
