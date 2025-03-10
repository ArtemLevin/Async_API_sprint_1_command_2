import logging
from functools import lru_cache
from typing import Annotated
from uuid import UUID

from fastapi import Depends
from pydantic import BaseModel

from src.core.config import settings
from src.core.exceptions import CheckCacheError, CheckElasticError
from src.db.elastic import get_elastic
from src.db.redis_client import get_redis
from src.models.models import FilmBase, Person
from src.services.base_service import BaseService
from src.utils.cache_service import CacheService
from src.utils.elastic_service import ElasticService

logger = logging.getLogger(__name__)


class PersonService(BaseService):
    """
    Сервис для работы с персонами.

    Осуществляет взаимодействие с Redis (для кеширования)
    и Elasticsearch (для полнотекстового поиска).
    """

    async def get_person_by_id(self, person_id: str) -> BaseModel | None:
        """Получить фильм по его ID."""
        log_info = f"Получение персоны по ID {person_id}"

        logger.info(log_info)

        #  Индекс для Elasticsearch
        es_index = "persons"
        # Ключ для кеша
        cache_key = f"person:{person_id}"
        # Модель Pydantic для возврата
        model = Person

        return await self._get_by_id(
            model, es_index, person_id, cache_key, log_info
        )

    async def get_person_films(
            self,
            person: UUID,
    ) -> list[BaseModel] | None:
        """
        Получить список фильмов в производстве которых участвовала персона.
        """
        log_info = (
            f"Запрос на получение фильмов с участием персоны: id = {person}."
        )

        logger.info(log_info)

        #  Индекс для Elasticsearch
        es_index = "persons"
        # Ключ для кеша
        cache_key = f"person_films:{person}"
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
        body = {"query": {"bool": {
            "should": [
                {
                    "nested": {
                        "path": "actors",
                        "query": {
                            "term": {
                                "actors.id": person,
                            }
                        }
                    }
                },
                {
                    "nested": {
                        "path": "writers",
                        "query": {
                            "term": {
                                "writers.id": person,
                            }
                        }
                    }
                },
                {
                    "nested": {
                        "path": "directors",
                        "query": {
                            "term": {
                                "directors.id": person,
                            }
                        }
                    }
                },
            ],
            "minimum_should_match": 1,
        }}, "size" : settings.ELASTIC_RESPONSE_SIZE}

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

    async def search_persons(
        self,
        query: str | None = None,
        page_size: int = 10,
        page_number: int = 1,
    ) -> list[BaseModel] | None:
        """
        Поиск персон по ключевым словам и пагинацией.
        """
        log_info = (
            f"Запрос на получение персон: (query={query}, "
            f"page_size={page_size}, page_number={page_number})."
        )

        logger.info(log_info)

        #  Индекс для Elasticsearch
        es_index = "persons"
        # Модель Pydantic для возврата
        model = Person

        # Формируем тело запроса для Elasticsearch
        body = {"query": {}}

        #  Поиск, если есть
        if query:
            body["query"]["multi_match"] = {
                "query": query,
                "fields": ["full_name"],
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
def get_person_service(
    redis: Annotated[CacheService, Depends(get_redis)],
    elastic: Annotated[ElasticService, Depends(get_elastic)]
) -> PersonService:
    """
    Провайдер для получения экземпляра PersonService.

    Функция создаёт синглтон экземпляр PersonService, используя Redis и
    Elasticsearch, которые передаются через Depends (зависимости FastAPI).

    :param redis: Экземпляр клиента Redis, предоставленный через Depends.
    :param elastic: Экземпляр клиента Elasticsearch, предоставленный через
    Depends.
    :return: Экземпляр PersonService, который используется для работы с
    фильмами.
    """
    logger.info(
        "Создаётся экземпляр PersonService с использованием Redis и "
        "Elasticsearch."
    )
    return PersonService(redis, elastic)
