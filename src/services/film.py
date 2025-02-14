import orjson
import logging
from elasticsearch import AsyncElasticsearch, helpers
from redis.asyncio import Redis
import asyncio

logger = logging.getLogger(__name__)

class FilmService:
    """
    Сервис для работы с фильмами.

    Осуществляет взаимодействие с Redis (для кэширования) и Elasticsearch (для полнотекстового поиска).
    """

    def __init__(self, redis: Redis, elastic: AsyncElasticsearch):
        """
        Инициализация FilmService.

        :param redis: Клиент Redis для работы с кэшем.
        :param elastic: Клиент Elasticsearch для работы с хранилищем данных.
        """
        self.redis = redis
        self.elastic = elastic

    async def get_film_by_id(self, film_id: str) -> dict:
        """
        Получить фильм по его ID.
        """
        logger.info("Получение фильма по ID: %s", film_id)

        # Проверяем наличие фильма в кэше (Redis)
        cached_film = await self.redis.get(f"film:{film_id}")
        if cached_film:
            logger.debug("Фильм найден в кэше: %s", film_id)
            return orjson.loads(cached_film)

        # Если фильма нет в кэше, ищем его в Elasticsearch
        logger.debug("Фильм не найден в кэше. Поиск в Elasticsearch: %s", film_id)
        film = await self._get_film_from_elastic(film_id)
        if not film:
            return None

        # Кэшируем фильм в Redis
        asyncio.create_task(self._cache_film(film_id, film))  # Кэшируем асинхронно
        return film

    async def search_films(self, query: str, limit: int = 10, offset: int = 0) -> list:
        """
        Поиск фильмов по ключевым словам.
        """
        logger.info("Поиск фильмов. Запрос: '%s', Лимит: %d, Смещение: %d", query, limit, offset)

        # Формируем тело запроса для Elasticsearch
        body = {
            "query": {
                "multi_match": {
                    "query": query,
                    "fields": ["title^5", "description^2", "actors^1", "genres^0.5"]
                }
            },
            "from": offset,
            "size": limit
        }

        try:
            response = await self.elastic.search(index="films", body=body)
            films = [hit["_source"] for hit in response["hits"]["hits"]]
            logger.info("Поиск завершён. Найдено фильмов: %d", len(films))
            return films
        except Exception as e:
            logger.error("Ошибка при поиске фильмов в Elasticsearch: %s", e)
            raise

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

    async def _get_film_from_elastic(self, film_id: str) -> dict:
        """
        Вспомогательный метод для получения фильма из Elasticsearch.
        """
        try:
            doc = await self.elastic.get(index="films", id=film_id, ignore=[404])
            if not doc.get("found"):
                logger.warning("Фильм не найден в Elasticsearch: %s", film_id)
                return None
            return doc["_source"]
        except Exception as e:
            logger.error("Ошибка при запросе к Elasticsearch: %s", e)
            raise

    async def _cache_film(self, film_id: str, film_data: dict) -> None:
        """
        Вспомогательный метод для кэширования фильма в Redis.
        """
        try:
            await self.redis.set(f"film:{film_id}", orjson.dumps(film_data), ex=3600)
            logger.debug("Фильм сохранён в кэше: %s", film_id)
        except Exception as e:
            logger.error("Ошибка при кэшировании фильма: %s", e)