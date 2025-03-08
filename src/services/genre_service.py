import logging
from functools import lru_cache
from typing import Annotated

from fastapi import Depends
from pydantic import BaseModel

from src.core.exceptions import CheckCacheError, CheckElasticError
from src.db.elastic import get_elastic
from src.db.redis_client import get_redis
from src.models.models import GenreBase
from src.services.base_service import BaseService
from src.utils.cache_service import CacheService
from src.utils.elastic_service import ElasticService

logger = logging.getLogger(__name__)


class GenreService(BaseService):
    """
    Сервис для работы с жанрами.

    Осуществляет взаимодействие с Redis (для кеширования)
    и Elasticsearch (для полнотекстового поиска).
    """

    async def get_genre_by_id(self, genre_id: str) -> BaseModel | None:
        """Получить фильм по его ID."""
        log_info = f"Получение жанра по ID {genre_id}"

        logger.info(log_info)

        #  Индекс для Elasticsearch
        es_index = "genres"
        # Ключ для кеша
        cache_key = f"genre:{genre_id}"
        # Модель Pydantic для возврата
        model = GenreBase

        return await self._get_by_id(
            model, es_index, genre_id, cache_key, log_info
        )

    async def get_genres(self) -> list[BaseModel] | None:
        """Получить список жанров."""
        log_info = "Запрос на получение списка жанров."

        logger.info(log_info)

        # Индекс для Elasticsearch
        es_index = "genres"
        # Ключ для кеша
        cache_key = "genres"
        # Модель Pydantic для возврата
        model = GenreBase

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
        body = {"query": {"match_all": {}}}

        # Проверяем наличие результата в Elasticsearch
        try:
            genres_obj = await self._get_records_from_elastic(
                model, es_index, body, log_info
            )

        except CheckElasticError:
            return None

        else:
            # Кешируем асинхронно результат в Redis
            await self._put_to_cache(cache_key, genres_obj, log_info)

            return genres_obj

    async def search_genres(
        self, query: str | None = None
    ) -> list[BaseModel] | None:
        """Поиск жанров по ключевым словам."""
        log_info = f"Запрос на получение жанров: (query={query})."

        logger.info(log_info)

        #  Индекс для Elasticsearch
        es_index = "genres"
        # Модель Pydantic для возврата
        model = GenreBase

        # Формируем тело запроса для Elasticsearch
        body = {"query": {}}

        #  Поиск, если есть
        if query:
            body["query"]["multi_match"] = {
                "query": query,
                "fields": ["name"],
                "fuzziness": "AUTO"
            }

        else:
            body["query"]["match_all"] = {}

        # Проверяем наличие результата в Elasticsearch
        try:
            return await self._get_records_from_elastic(
                model, es_index, body, log_info
            )

        except CheckElasticError:
            return None


@lru_cache()
def get_genre_service(
    redis: Annotated[CacheService, Depends(get_redis)],
    elastic: Annotated[ElasticService, Depends(get_elastic)]
) -> GenreService:
    """
    Провайдер для получения экземпляра GenreService.

    Функция создаёт синглтон экземпляр GenreService, используя Redis и
    Elasticsearch, которые передаются через Depends (зависимости FastAPI).

    :param redis: Экземпляр клиента Redis, предоставленный через Depends.
    :param elastic: Экземпляр клиента Elasticsearch, предоставленный через
    Depends.
    :return: Экземпляр GenreService, который используется для работы с
    фильмами.
    """
    logger.info(
        "Создаётся экземпляр GenreService с использованием Redis и "
        "Elasticsearch."
    )
    return GenreService(redis, elastic)
