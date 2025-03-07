import logging
from decimal import Decimal
from typing import List
from uuid import UUID

from src.models.models import Film, GenreBase, PersonBase
from src.services.base_service import BaseService
from src.utils.decorators import redis_cache

logger = logging.getLogger(__name__)


class FilmService(BaseService):
    """
    Сервис для работы с данными о фильмах.
    """
    def __init__(self, elastic_service, cache_service, index_name: str):
        super().__init__(elastic_service, cache_service, index_name)
        self.logger = logging.getLogger(__name__)

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

    @redis_cache(key_prefix="films")
    async def search_films_by_title(self, query: str, page_size: int = 10, page_number: int = 1) -> List[Film]:
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
                        "title": query                 # Ищем по полю "title"
                    }
                }
            }

            logger.info(f"Выполняем поиск фильмов по запросу: {query}")

            # Выполняем запрос к Elasticsearch
            response = await self.elastic_service.search(index="films", body=search_query)

            # Фильтруем только успешные результаты
            films = [
                self.parse_elastic_response(hit)
                for hit in response.get("hits", {}).get("hits", [])
                if self.parse_elastic_response(hit) is not None
            ]

            logger.info(f"Найдено {len(films)} фильмов по запросу '{query}'.")
            return films

        except Exception as e:
            logger.error(f"Ошибка при поиске фильмов по запросу '{query}': {e}")
            raise


    async def search_films_by_description(self, query: str, page_size: int = 10, page_number: int = 1) -> List[Film]:
        """
        Поиск фильмов по названию.

        :param query: Строка для поиска (description).
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
                        "description": query                 # Ищем по полю "description"
                    }
                }
            }

            logger.info(f"Выполняем поиск фильмов по запросу: {query}")

            # Выполняем запрос к Elasticsearch
            response = await self.elastic_service.search(index="films", body=search_query)

            # Фильтруем только успешные результаты
            films = [
                self.parse_elastic_response(hit)
                for hit in response.get("hits", {}).get("hits", [])
                if self.parse_elastic_response(hit) is not None
            ]

            logger.info(f"Найдено {len(films)} фильмов по запросу '{query}'.")
            return films

        except Exception as e:
            logger.error(f"Ошибка при поиске фильмов по запросу '{query}': {e}")
            raise


    async def search_films_by_actor(self, actor_name: str, page_size: int = 10, page_number: int = 1) -> List[Film]:
        """
        Поиск фильмов по имени актёра.

        :param actor_name: Строка для поиска (full_name актёра).
        :param page_size: Количество результатов на странице.
        :param page_number: Номер страницы.
        :return: Список объектов Film.
        """
        try:
            # Формируем запрос для поиска по имени актёра
            search_query = {
                "from": (page_number - 1) * page_size,  # Пагинация: начало выборки
                "size": page_size,  # Пагинация: размер страницы
                "query": {
                    "nested": {
                        "path": "actors",  # указываем путь к вложенному объекту
                        "query": {
                            "match": {
                                "actors.full_name": actor_name  # выполняем поиск по имени актёра
                            }
                        }
                    }
                }
            }

            logger.info(f"Выполняем поиск фильмов с актёром: {actor_name}")

            # Выполняем запрос к Elasticsearch
            response = await self.elastic_service.search(index="films", body=search_query)

            # Обрабатываем и фильтруем результаты запроса
            films = [
                self.parse_elastic_response(hit)
                for hit in response.get("hits", {}).get("hits", [])
                if self.parse_elastic_response(hit) is not None
            ]

            logger.info(f"Найдено {len(films)} фильмов с актёром '{actor_name}'.")
            return films

        except Exception as e:
            logger.error(f"Ошибка при поиске фильмов по имени актёра '{actor_name}': {e}")
            raise


    async def search_films_by_writer(self, writer_name: str, page_size: int = 10, page_number: int = 1) -> List[Film]:
        """
        Поиск фильмов по имени сценариста (writer).

        :param writer_name: Строка для поиска (full_name сценариста).
        :param page_size: Количество результатов на странице.
        :param page_number: Номер страницы.
        :return: Список объектов Film.
        """
        try:
            # Формируем запрос с использованием nested-запроса для поля "writers"
            search_query = {
                "from": (page_number - 1) * page_size,  # Пагинация: начало выборки
                "size": page_size,                      # Пагинация: размер страницы
                "query": {
                    "nested": {
                        "path": "writers",  # Указываем путь к вложенному объекту "writers"
                        "query": {
                            "match": {
                                "writers.full_name": writer_name  # Поиск по полю "writers.full_name"
                            }
                        }
                    }
                }
            }

            logger.info(f"Выполняем поиск фильмов с сценаристом: {writer_name}")

            # Выполняем запрос к Elasticsearch
            response = await self.elastic_service.search(index="films", body=search_query)

            # Обрабатываем результаты запроса
            films = [
                self.parse_elastic_response(hit)
                for hit in response.get("hits", {}).get("hits", [])
                if self.parse_elastic_response(hit) is not None
            ]

            logger.info(f"Найдено {len(films)} фильмов с сценаристом '{writer_name}'.")
            return films

        except Exception as e:
            logger.error(f"Ошибка при поиске фильмов по имени сценариста '{writer_name}': {e}")
            raise


    async def search_films_by_director(self, director_name: str, page_size: int = 10, page_number: int = 1) -> List[Film]:
        """
        Поиск фильмов по имени режиссёра (director).

        :param director_name: Строка для поиска (full_name режиссёра).
        :param page_size: Количество результатов на странице.
        :param page_number: Номер страницы.
        :return: Список объектов Film.
        """
        try:
            # Формируем запрос с использованием nested-запроса для поля "directors"
            search_query = {
                "from": (page_number - 1) * page_size,  # Пагинация: начало выборки
                "size": page_size,                      # Пагинация: размер страницы
                "query": {
                    "nested": {
                        "path": "directors",  # указываем путь к вложенному объекту "directors"
                        "query": {
                            "match": {
                                "directors.full_name": director_name  # поиск по полю "directors.full_name"
                            }
                        }
                    }
                }
            }

            logger.info(f"Выполняем поиск фильмов с режиссёром: {director_name}")

            # Выполняем запрос к Elasticsearch
            response = await self.elastic_service.search(index="films", body=search_query)

            # Обрабатываем результаты запроса
            films = [
                self.parse_elastic_response(hit)
                for hit in response.get("hits", {}).get("hits", [])
                if self.parse_elastic_response(hit) is not None
            ]

            logger.info(f"Найдено {len(films)} фильмов с режиссёром '{director_name}'.")
            return films

        except Exception as e:
            logger.error(f"Ошибка при поиске фильмов по имени режиссёра '{director_name}': {e}")
            raise


    async def search_films_by_genre(self, genre_name: str, page_size: int = 10, page_number: int = 1) -> List[Film]:
        """
        Поиск фильмов по названию жанра.

        :param genre_name: Строка для поиска (название жанра, поле genre.name).
        :param page_size: Количество результатов на странице.
        :param page_number: Номер страницы.
        :return: Список объектов Film.
        """
        try:
            # Формируем запрос с использованием nested-запроса по полю "genre"
            search_query = {
                "from": (page_number - 1) * page_size,  # Пагинация: начало выборки
                "size": page_size,                      # Пагинация: размер страницы
                "query": {
                    "nested": {
                        "path": "genre",  # указываем путь к вложенному объекту "genre"
                        "query": {
                            "match": {
                                "genre.name": genre_name  # Поиск по полю "genre.name"
                            }
                        }
                    }
                }
            }

            logger.info(f"Выполняем поиск фильмов с жанром: {genre_name}")

            # Выполняем запрос к Elasticsearch
            response = await self.elastic_service.search(index="films", body=search_query)

            # Обрабатываем результаты запроса
            films = [
                self.parse_elastic_response(hit)
                for hit in response.get("hits", {}).get("hits", [])
                if self.parse_elastic_response(hit) is not None
            ]

            logger.info(f"Найдено {len(films)} фильмов с жанром '{genre_name}'.")
            return films

        except Exception as e:
            logger.error(f"Ошибка при поиске фильмов по жанру '{genre_name}': {e}")
            raise