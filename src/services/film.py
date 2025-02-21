import asyncio
import logging
from functools import lru_cache
from typing import Annotated, Optional

import orjson
from elasticsearch import AsyncElasticsearch
from elasticsearch import BadRequestError as ESBadRequestError
from elasticsearch import ConnectionError as ESConnectionError
from elasticsearch import ConnectionTimeout as ESConnectionTimeout
from elasticsearch import NotFoundError
from elasticsearch import TransportError as ESTransportError
from elasticsearch import helpers
from fastapi import Depends
from pydantic import ValidationError
from redis.asyncio import Redis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import RedisError
from redis.exceptions import TimeoutError as RedisTimeoutError

from src.db.elastic import get_elastic
from src.db.redis import get_redis
from src.models.models import Film

ELASTIC_INDEX = "films"
FILM_CACHE_EXPIRE_IN_SECONDS = 300
ELASTIC_EXCEPTIONS = (
    ESConnectionError, ESConnectionTimeout, ESTransportError, ESBadRequestError
)
REDIS_EXCEPTIONS = (RedisConnectionError, RedisTimeoutError, RedisError)
GET_FILM_BY_ID_EXCLUDE = {}
GET_FILMS_EXCLUDE = {"description", "genre", "actors", "writers", "directors"}

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

        :param redis: Клиент Redis для работы с кешем.
        :param elastic: Клиент Elasticsearch для работы с хранилищем данных.
        """
        self.redis = redis
        self.elastic = elastic

    @staticmethod
    def _model_dump(
            film: Film, exclude: set | dict | None = None
    ) -> Optional[dict]:
        """
        Вспомогательный метод для генерации словаря из объекта модели Film.
        """
        try:
            return film.model_dump(by_alias=True, exclude=exclude)

        except (TypeError, ValueError, KeyError) as e:
            logger.error("Ошибка при сериализации фильма %s: %s", film.id, e)

    @staticmethod
    def _create_film_object(data: dict) -> Optional[Film]:
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
    ) -> Optional[list[dict]]:
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
                ex=FILM_CACHE_EXPIRE_IN_SECONDS,
            )

        except REDIS_EXCEPTIONS as e:
            logger.error(
                "Ошибка при записи кеша с ключом %s в Redis: %s",
                cache_key, e
            )

        else:
            logger.debug("Кеш с ключом %s сохранён в Redis..", cache_key)

    async def _get_from_cache(
            self, cache_key: str
    ) -> (list[dict] | dict | None):
        """
        Вспомогательный метод для получения фильма из кеша.
        """
        try:
            cache_data = await self.redis.get(cache_key)

        except REDIS_EXCEPTIONS as e:
            logger.error("Ошибка при чтении кеша из Redis: %s", e)

        else:
            return orjson.loads(cache_data)

    async def _get_film_from_elastic(self, film_id: str) -> Optional[dict]:
        """
        Вспомогательный метод для получения фильма из Elasticsearch.
        """
        try:
            film = await self.elastic.get(index=ELASTIC_INDEX, id=film_id)

        except NotFoundError:
            logger.warning("Фильм c ID %s не найден в Elasticsearch.", film_id)

        except ELASTIC_EXCEPTIONS as e:
            logger.error(
                "Ошибка при запросе к Elasticsearch для поиска фильма с ID "
                "%s: %s",
                film_id, e
            )

        else:
            return film['_source']

    async def _get_films_from_elastic(
            self, body: dict
    ) -> Optional[list[dict]]:
        """
        Вспомогательный метод для получения фильмов из Elasticsearch.
        """
        try:
            films = await self.elastic.search(index=ELASTIC_INDEX, body=body)

        except ELASTIC_EXCEPTIONS as e:
            logger.error("Ошибка при запросе к Elasticsearch: %s", e)

        else:
            return films["hits"]["hits"]

    async def get_film_by_id(self, film_id: str) -> Optional[dict]:
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
        film_dump = self._model_dump(film_obj, GET_FILM_BY_ID_EXCLUDE)

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
            limit: int = 10,
            offset: int = 0,
    ) -> Optional[list[dict]]:
        """
        Получить список фильмов с поддержкой сортировки по рейтингу,
        фильтрации по жанру и пагинацией.

        :param genre: UUID жанра для фильтрации (пример: <comedy-uuid>).
        :param sort: Поле для сортировки (пример: "-imdb_rating" для убывания).
        :param limit: Количество фильмов в результате (по умолчанию 10).
        :param offset: Смещение для пагинации (по умолчанию 0).
        :return: Список фильмов, соответствующих критериям.
        """
        logger.info(
            "Запрос на получение фильмов: "
            "sort=%s, genre=%s, limit=%d, offset=%d",
            sort, genre, limit, offset
        )

        cache_key = f"films:{sort}:{genre}:{limit}:{offset}"  # Ключ для кеша

        # Проверяем наличие результата в кеше (Redis)
        if cached_films := await self._get_from_cache(cache_key):
            logger.debug(
                "Фильмы по запросу (sort=%s, genre=%s, limit=%d, offset=%d) "
                "найдены в кеше.",
                sort, genre, limit, offset
            )
            return cached_films

        # Если нет в кеше, ищем в Elasticsearch
        # Фильтр по жанру, если указан
        filter_by_genre = [{"term": {"genres.id": genre}}] if genre else []

        # Сортировка
        sort_field = sort.lstrip("-")
        sort_order = "desc" if sort.startswith("-") else "asc"
        sort = [{sort_field: sort_order}],

        # Формируем тело запроса для Elasticsearch
        body = {
            "query": {
                "bool": {
                    "must": [],
                    "filter": filter_by_genre,
                },
            },
            "sort": sort,
            "from": offset,
            "size": limit
        }

        # Выполняем запрос к Elasticsearch
        films = await self._get_films_from_elastic(body)

        logger.debug(
            "Фильмов по запросу (sort=%s, genre=%s, limit=%d, offset=%d) "
            "найдено в Elasticsearch %d шт.",
            sort, genre, limit, offset, len(films)
        )

        if len(films) == 0:
            return None

        # Проверяем валидность полученных данных из Elasticsearch и формируем
        # словари с нужными ключами из объектов модели Film
        valid_films = self._validate_films(films, GET_FILMS_EXCLUDE)

        if len(valid_films) == 0:
            logger.warning(
                "По запросу (sort=%s, genre=%s, limit=%d, offset=%d) "
                "полученные фильмы из Elasticsearch не прошли валидацию",
                sort, genre, limit, offset,
            )
            return None

        # Кешируем асинхронно результат в Redis
        logger.info(
            "Кеширование запроса на получение фильмов: "
            "sort=%s, genre=%s, limit=%d, offset=%d",
            sort, genre, limit, offset
        )

        json_represent = orjson.dumps(valid_films)

        asyncio.create_task(self._put_film_to_cache(
            cache_key, json_represent
        ))

        logger.info(
            "Фильмы по запросу (sort=%s, genre=%s, limit=%d, offset=%d) "
            "успешно получены. Количество: %d",
            sort, genre, limit, offset, len(valid_films)
        )

        return valid_films

    async def search_films(
            self, query: str, limit: int = 10, offset: int = 0
    ) -> Optional[list[dict]]:
        """
        Поиск фильмов по ключевым словам.

        :param query: Поисковый запрос (строка, обязательный).
        :param limit: Количество фильмов в результате (по умолчанию 10).
        :param offset: Смещение для пагинации (по умолчанию 0).
        :return: Список фильмов, соответствующих критериям.
        """
        logger.info(
            "Запрос на получение фильмов: query=%s, limit=%d, offset=%d",
            query, limit, offset
        )

        # Формируем тело запроса для Elasticsearch
        body = {
            "query": {
                "multi_match": {
                    "query": query,
                    "fields": [
                        "title^5", "description^2", "actors^1", "genres^0.5"
                    ],
                },
            },
            "from": offset,
            "size": limit
        }

        # Выполняем запрос к Elasticsearch
        films = await self._get_films_from_elastic(body)

        logger.debug(
            "Фильмов по запросу (query=%s, limit=%d, offset=%d) найдены в "
            "найдено в Elasticsearch %d шт.",
            query, limit, offset, len(films)
        )

        if len(films) == 0:
            return None

        # Проверяем валидность полученных данных из Elasticsearch и формируем
        # словари с нужными ключами из объектов модели Film
        valid_films = self._validate_films(films, GET_FILMS_EXCLUDE)

        if len(valid_films) == 0:
            logger.warning(
                "По запросу (query=%s, limit=%d, offset=%d) полученные фильмы "
                "из Elasticsearch не прошли валидацию",
                query, limit, offset,
            )
            return None

        logger.info(
            "Фильмы по запросу (query=%s, limit=%d, offset=%d) "
            "успешно получены. Количество: %d",
            query, limit, offset, len(films)
        )

        return valid_films

    async def add_film(self, film_id: str, film_data: dict) -> None:
        """
        Добавить новый фильм.
        """
        logger.info("Добавление фильма. ID: %s", film_id)

        # Используем asyncio.gather для параллельного добавления в Elasticsearch и Redis
        try:
            await asyncio.gather(
                self.elastic.index(index="films", id=film_id, body=film_data),
                self.redis.set(f"film:{film_id}", orjson.dumps(film_data), ex=3600)
            )
            logger.info("Фильм добавлен: %s", film_id)
        except Exception as e:
            logger.error("Ошибка при добавлении фильма. ID: %s, Ошибка: %s", film_id, e)
            raise

    async def delete_film(self, film_id: str) -> None:
        """
        Удалить фильм.
        """
        logger.info("Удаление фильма. ID: %s", film_id)

        # Используем asyncio.gather для параллельного удаления из Elasticsearch и Redis
        try:
            await asyncio.gather(
                self.elastic.delete(index="films", id=film_id, ignore=[404]),
                self.redis.delete(f"film:{film_id}")
            )
            logger.info("Фильм удалён: %s", film_id)
        except Exception as e:
            logger.error("Ошибка при удалении фильма. ID: %s, Ошибка: %s", film_id, e)
            raise

    async def bulk_add_films(self, films: list[dict]) -> None:
        """
        Добавить несколько фильмов одновременно.
        """
        logger.info("Массовое добавление фильмов. Количество: %d", len(films))

        # Формируем действия для Elasticsearch Bulk API
        elastic_actions = [
            {
                "_index": "films",
                "_id": film["id"],
                "_source": film
            }
            for film in films
        ]

        # Кэшируем фильмы в Redis через Pipeline
        redis_pipeline = self.redis.pipeline()
        for film in films:
            redis_pipeline.set(f"film:{film['id']}", orjson.dumps(film), ex=3600)

        # Выполняем массовые операции параллельно
        try:
            await asyncio.gather(
                helpers.async_bulk(self.elastic, elastic_actions),
                redis_pipeline.execute()
            )
            logger.info("Массовое добавление завершено.")
        except Exception as e:
            logger.error("Ошибка при массовом добавлении фильмов: %s", e)
            raise


@lru_cache()
def get_film_service(
        redis: Annotated[Redis, Depends(get_redis)],
        elastic: Annotated[AsyncElasticsearch, Depends(get_elastic)]
) -> FilmService:
    """
    Провайдер для получения экземпляра FilmService.

    Функция создаёт синглтон экземпляр FilmService, используя Redis и Elasticsearch,
    которые передаются через Depends (зависимости FastAPI).

    :param redis: Экземпляр клиента Redis, предоставленный через Depends.
    :param elastic: Экземпляр клиента Elasticsearch, предоставленный через Depends.
    :return: Экземпляр FilmService, который используется для работы с фильмами.
    """
    logger.info("Создаётся экземпляр FilmService с использованием Redis и Elasticsearch.")
    return FilmService(redis, elastic)
