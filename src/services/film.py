import asyncio
import logging
from functools import lru_cache
from typing import Annotated

import orjson
from elasticsearch import AsyncElasticsearch, NotFoundError
from fastapi import Depends
from pydantic import ValidationError
from redis.asyncio import Redis

from src.core.config import settings
from src.db.elastic import get_elastic
from src.db.redis_client import get_redis
from src.models.models import Film

logger = logging.getLogger(__name__)


class FilmService:
    """
    Сервис для работы с фильмами.

    Осуществляет взаимодействие с Redis (для кеширования)
    и Elasticsearch (для полнотекстового поиска).
    """

    def __init__(self, redis: Redis, elastic: AsyncElasticsearch):
        """
        Инициализация FilmService.
        """
        self.redis = redis
        self.elastic = elastic

    @staticmethod
    def _model_dump(
            film: Film, exclude: set | dict | None = None
    ) -> dict | None:
        """
        Вспомогательный метод для генерации словаря из объекта модели Film.
        """
        try:
            return film.model_dump(by_alias=True, exclude=exclude)

        except (TypeError, ValueError, KeyError) as e:
            logger.error("Ошибка при сериализации фильма %s: %s", film.id, e)

    @staticmethod
    def _create_film_object(data: dict) -> Film | None:
        """Вспомогательный метод для создания объекта модели Film."""
        try:
            film = Film(**data)

        except ValidationError as e:
            logger.error(
                "Ошибка при валидации фильма из Elasticsearch: %s. "
                "Данные для валидации: %s",
                e.json(), data
            )

        else:
            return film

    def _validate_films(
            self, films: list[dict], exclude: set | dict | None = None
    ) -> list[dict] | None:
        """
        Вспомогательный метод для валидации данных по фильмам и формирования
        списка словарей с требуемыми полями фильмов.
        """
        valid_films = []

        for film in films:
            if film_obj := self._create_film_object(film):
                if film_dump := self._model_dump(film_obj, exclude):
                    valid_films.append(film_dump)

        return valid_films

    async def _put_film_to_cache(
            self, cache_key: str, json_data: bytes
    ) -> None:
        """Вспомогательный метод для кеширования."""
        try:
            await self.redis.set(
                cache_key,
                json_data,
                ex=settings.FILM_CACHE_EXPIRE_IN_SECONDS,
            )

        except settings.REDIS_EXCEPTIONS as e:
            logger.error(
                "Ошибка при записи кеша с ключом %s в Redis: %s",
                cache_key, e
            )

        else:
            logger.debug("Кеш с ключом %s сохранён в Redis.", cache_key)

    async def _get_from_cache(
            self, cache_key: str
    ) -> (list[dict] | dict | None):
        """
        Вспомогательный метод для получения фильма из кеша.
        """
        try:
            cache_data = await self.redis.get(cache_key)

        except settings.REDIS_EXCEPTIONS as e:
            logger.error("Ошибка при чтении кеша из Redis: %s", e)

        else:
            try:
                return orjson.loads(cache_data)

            except orjson.JSONDecodeError:
                return None

    async def _get_film_from_elastic(self, film_id: str) -> dict | None:
        """
        Вспомогательный метод для получения фильма из Elasticsearch.
        """
        try:
            film = await self.elastic.get(
                index=settings.ELASTIC_INDEX, id=film_id
            )

        except NotFoundError:
            logger.warning("Фильм c ID %s не найден в Elasticsearch.", film_id)

        except settings.ELASTIC_EXCEPTIONS as e:
            logger.error(
                "Ошибка при запросе к Elasticsearch для поиска фильма с ID "
                "%s: %s",
                film_id, e
            )

        else:
            try:
                return film['_source']
            except (KeyError, TypeError) as e:
                logger.error(
                    "Ошибка некорректного ответа от Elasticsearch для поиска "
                    "фильма с ID %s: %s",
                    film_id, e
                )

    async def _get_films_from_elastic(
            self, body: dict
    ) -> list[dict] | None:
        """
        Вспомогательный метод для получения фильмов из Elasticsearch.
        """
        try:
            films = await self.elastic.search(
                index=settings.ELASTIC_INDEX, body=body
            )

        except NotFoundError:
            logger.warning(
                "Фильмы не найдены в Elasticsearch. 'body' запроса: %s",
                body,
            )

        except settings.ELASTIC_EXCEPTIONS as e:
            logger.error(
                "Ошибка при запросе к Elasticsearch: %s. 'body' запроса: %s",
                e, body
            )

        else:
            try:
                return films["hits"]["hits"]
            except (KeyError, TypeError) as e:
                logger.error(
                    "Ошибка некорректного ответа от Elasticsearch: %s."
                    " 'body' запроса: %s",
                    e, body
                )

        return []

    async def get_film_by_id(self, film_id: str) -> dict | None:
        """
        Получить фильм по его ID.
        """
        logger.info("Получение фильма по ID: %s", film_id)
        cache_key = f"film:{film_id}"  # Ключ для кеша

        # Проверяем наличие фильма в кеше (Redis)
        if cached_film := await self._get_from_cache(cache_key):
            logger.debug("Фильм найден в кеше: %s", film_id)
            return cached_film

        # Если фильма нет в кеше, ищем в Elasticsearch
        film = await self._get_film_from_elastic(film_id)

        # Проверяем валидность полученных данных из Elasticsearch
        film_obj = self._create_film_object(film)

        if not film_obj:
            return None

        logger.debug("Фильм найден в Elasticsearch: %s", film_id)

        # Формируем словарь с нужными ключами из объекта модели Film
        film_dump = self._model_dump(film_obj, settings.GET_FILM_BY_ID_EXCLUDE)

        if not film_dump:
            return None

        # Кешируем асинхронно фильм в Redis
        logger.info("Кеширование фильма: %s", film_id)

        json_represent = orjson.dumps(film_dump)

        asyncio.create_task(self._put_film_to_cache(
            cache_key, json_represent
        ))

        return film_dump

    async def get_films(
            self,
            genre: str = None,
            sort: str = "-imdb_rating",
            page_size: int = 10,
            page_number: int = 1,
    ) -> list[dict] | None:

        """
        Получить список фильмов с поддержкой сортировки по рейтингу,
        фильтрации по жанру и пагинацией.
        """
        logger.info(
            "Запрос на получение фильмов: "
            "sort=%s, genre=%s, page_size=%d, page_number=%d",
            sort, genre, page_size, page_number
        )

        # Ключ для кеша
        cache_key = f"films:{sort}:{genre}:{page_size}:{page_number}"

        # Проверяем наличие результата в кеше (Redis)
        if cached_films := await self._get_from_cache(cache_key):
            logger.debug(
                "Фильмы по запросу (sort=%s, genre=%s, page_size=%d, "
                "page_number=%d) найдены в кеше.",
                sort, genre, page_size, page_number
            )
            return cached_films

        # Если нет в кеше, ищем в Elasticsearch
        # Фильтр по жанру, если указан
        filter_by_genre = [{"term": {"genres.id": genre}}] if genre else []

        # Сортировка
        sort_field = sort.lstrip("-")
        sort_order = "desc" if sort.startswith("-") else "asc"
        sort = [{sort_field: sort_order}]

        # Формируем тело запроса для Elasticsearch
        from_value = (page_number - 1) * page_size

        body = {
            "query": {
                "bool": {
                    "must": [],
                    "filter": filter_by_genre,
                },
            },
            "sort": sort,
            "from": from_value,
            "size": page_size,
        }

        # Выполняем запрос к Elasticsearch
        films = await self._get_films_from_elastic(body)

        logger.debug(
            "Фильмов по запросу (sort=%s, genre=%s, page_size=%d, "
            "page_number=%d) найдено в Elasticsearch %d шт.",
            sort, genre, page_size, page_number, len(films)
        )

        if not films:
            return None

        # Проверяем валидность полученных данных из Elasticsearch и формируем
        # словари с нужными ключами из объектов модели Film
        valid_films = self._validate_films(films, settings.GET_FILMS_EXCLUDE)

        if not valid_films:
            logger.warning(
                "По запросу (sort=%s, genre=%s, page_size=%d, page_number=%d) "
                "полученные фильмы из Elasticsearch не прошли валидацию",
                sort, genre, page_size, page_number
            )
            return None

        # Кешируем асинхронно результат в Redis
        logger.info(
            "Кеширование запроса на получение фильмов: "
            "sort=%s, genre=%s, page_size=%d, page_number=%d",
            sort, genre, page_size, page_number
        )

        json_represent = orjson.dumps(valid_films)

        asyncio.create_task(self._put_film_to_cache(
            cache_key, json_represent
        ))

        logger.info(
            "Фильмы по запросу (sort=%s, genre=%s, page_size=%d, "
            "page_number=%d) успешно получены. Количество: %d",
            sort, genre, page_size, page_number, len(valid_films)
        )

        return valid_films

    async def search_films(
            self, query: str, page_size: int = 10, page_number: int = 1
    ) -> list[dict] | None:
        """
        Поиск фильмов по ключевым словам.
        """
        logger.info(
            "Запрос на получение фильмов: "
            "query=%s, page_size=%d, page_number=%d",
            query, page_size, page_number
        )

        # Формируем тело запроса для Elasticsearch
        from_value = (page_number - 1) * page_size

        body = {
            "query": {
                "multi_match": {
                    "query": query,
                    "fields": [
                        "title^5", "description^2", "actors^1", "genres^0.5"
                    ],
                },
            },
            "from": from_value,
            "size": page_size,
        }

        # Выполняем запрос к Elasticsearch
        films = await self._get_films_from_elastic(body)

        logger.debug(
            "Фильмов по запросу (query=%s, page_size=%d, page_number=%d) "
            "найдено в Elasticsearch %d шт.",
            query, page_size, page_number, len(films)
        )

        if not films:
            return None

        # Проверяем валидность полученных данных из Elasticsearch и формируем
        # словари с нужными ключами из объектов модели Film
        valid_films = self._validate_films(films, settings.GET_FILMS_EXCLUDE)

        if not valid_films:
            logger.warning(
                "По запросу (query=%s, page_size=%d, page_number=%d) "
                "полученные фильмы из Elasticsearch не прошли валидацию",
                query, page_size, page_number
            )
            return None

        logger.info(
            "Фильмы по запросу (query=%s, page_size=%d, page_number=%d) "
            "успешно получены. Количество: %d",
            query, page_size, page_number, len(films)
        )

        return valid_films


@lru_cache()
def get_film_service(
        redis: Annotated[Redis, Depends(get_redis)],
        elastic: Annotated[AsyncElasticsearch, Depends(get_elastic)]
) -> FilmService:
    """
    Провайдер для получения экземпляра FilmService.

    Функция создаёт синглтон экземпляр FilmService, используя Redis и
    Elasticsearch, которые передаются через Depends (зависимости FastAPI).

    :param redis: Экземпляр клиента Redis, предоставленный через Depends.
    :param elastic: Экземпляр клиента Elasticsearch, предоставленный через
    Depends.
    :return: Экземпляр FilmService, который используется для работы с фильмами.
    """
    logger.info(
        "Создаётся экземпляр FilmService с использованием Redis и "
        "Elasticsearch."
    )
    return FilmService(redis, elastic)
