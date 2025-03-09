import logging
from functools import lru_cache
from typing import Annotated
from uuid import UUID

from fastapi import Depends
from pydantic import BaseModel

from src.core.exceptions import CheckCacheError, CheckElasticError
from src.db.elastic import get_elastic
from src.db.redis_client import get_redis
from src.models.models import Film, FilmBase
from src.services.base_service import BaseService
from src.utils.cache_service import CacheService
from src.utils.elastic_service import ElasticService

logger = logging.getLogger(__name__)


class FilmService(BaseService):
    """
    Сервис для работы с фильмами.

    Осуществляет взаимодействие с Redis (для кеширования)
    и Elasticsearch (для полнотекстового поиска).
    """

    async def get_film_by_id(self, film_id: str) -> BaseModel | None:
        """Получить фильм по его ID."""
        log_info = f"Получение фильма по ID {film_id}"

        logger.info(log_info)

        #  Индекс для Elasticsearch
        es_index = "films"
        # Ключ для кеша
        cache_key = f"film:{film_id}"
        # Модель Pydantic для возврата
        model = Film

        return await self._get_by_id(
            model, es_index, film_id, cache_key, log_info
        )

    async def get_films(
            self,
            genre: UUID | None = None,
            sort: str = "-imdb_rating",
            page_size: int = 10,
            page_number: int = 1,
    ) -> list[BaseModel] | None:
        """
        Получить список фильмов с поддержкой сортировки по рейтингу,
        фильтрации по жанру и пагинацией.
        """
        log_info = (
            f"Запрос на получение фильмов: (sort={sort}, genre={genre}, "
            f"page_size={page_size}, page_number={page_number})."
        )

        logger.info(log_info)

        #  Индекс для Elasticsearch
        es_index = "films"
        # Ключ для кеша
        cache_key = f"films:{genre}:{sort}:{page_size}:{page_number}"
        # Модель Pydantic для возврата
        model = FilmBase

        # Проверяем наличие результата в кеше (Redis)
        try:
            cache_films = await self._get_from_cache(
                model, cache_key, log_info
            )
            return cache_films

        except CheckCacheError:
            pass

        # Если нет в кеше, ищем в Elasticsearch

        # Формируем тело запроса для Elasticsearch
        body = {"query": {}}

        # Фильтр по жанру, если есть
        if genre:
            filter_by_genre = [
                {"nested": {
                    "path": "genres",
                    "query": {"term": {"genres.id": genre}},
                }}
            ]
            body["query"]["bool"] = {
                "must": [],
                "filter": filter_by_genre,
            }

        else:
            body["query"]["match_all"] = {}

        # Сортировка
        sort_field = sort.lstrip("-")
        sort_order = "desc" if sort.startswith("-") else "asc"

        body["sort"] = [{sort_field: {
            "order": sort_order, "missing": "_last"
        }}]

        #  Вычисляем начальную запись для выдачи
        from_value = (page_number - 1) * page_size

        body["from"] = from_value
        body["size"] = page_size

        # Проверяем наличие результата в Elasticsearch
        try:
            films_obj = await self._get_records_from_elastic(
                model, es_index, body, log_info
            )

        except CheckElasticError:
            return None

        else:
            # Кешируем асинхронно результат в Redis
            await self._put_to_cache(cache_key, films_obj, log_info)

            return films_obj

    async def search_films(
        self,
        query: str | None = None,
        page_size: int = 10,
        page_number: int = 1,
    ) -> list[BaseModel] | None:
        """
        Поиск фильмов по ключевым словам и пагинацией.
        """
        log_info = (
            f"Запрос на получение фильмов: (query={query}, "
            f"page_size={page_size}, page_number={page_number})."
        )

        logger.info(log_info)

        #  Индекс для Elasticsearch
        es_index = "films"
        # Модель Pydantic для возврата
        model = FilmBase

        # Формируем тело запроса для Elasticsearch
        body = {"query": {}}

        #  Поиск, если есть
        if query:
            body["query"]["multi_match"] = {
                "query": query,
                "fields": ["title"],
                "fuzziness": "AUTO"
            }

        else:
            body["query"]["match_all"] = {}

        #  Вычисляем начальную запись для выдачи
        from_value = (page_number - 1) * page_size

        body["from"] = from_value
        body["size"] = page_size

        # Проверяем наличие результата в Elasticsearch
        try:
            return await self._get_records_from_elastic(
                model, es_index, body, log_info
            )

        except CheckElasticError:
            return None


@lru_cache()
def get_film_service(
    redis: Annotated[CacheService, Depends(get_redis)],
    elastic: Annotated[ElasticService, Depends(get_elastic)]
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
