import logging
from typing import Dict, List

from elasticsearch import ConnectionError, RequestError, helpers
from more_itertools import chunked

from src.core.config import Settings
from src.utils.convert import convert_decimals
from src.utils.elastic_service import ElasticService

logger = logging.getLogger(__name__)

settings = Settings()

# Определяем константу для размера одной партии в bulk-операциях
BULK_CHUNK_SIZE = 10  # Максимальное количество документов за один запрос


class AddFilmService:
    def __init__(self, elastic: ElasticService):
        if elastic is None:
            raise RuntimeError("Elasticsearch клиент не инициализирован")
        self.elastic = elastic

    async def add_film(self, film_id: str, film_data: dict) -> None:
        logger.info("Добавление фильма. ID: %s", film_id)

        # Конвертируем все Decimal в float
        film_data = convert_decimals(film_data)

        try:
            logger.info("Перед вызовом self.elastic.index")
            await self.elastic.index(
                index=settings.ELASTIC_INDEX,
                id=film_id,
                body=film_data,
            )
            logger.info(
                "Фильм успешно добавлен в индекс %s: %s",
                settings.ELASTIC_INDEX, film_id
            )

        except ConnectionError as e:
            logger.error("Ошибка подключения к Elasticsearch: %s", e)
            raise
        except RequestError as e:
            logger.error("Ошибка запроса к Elasticsearch: %s", e.info)
            raise
        except Exception as e:
            logger.error("Неизвестная ошибка: %s", e)
            raise RuntimeError(f"Не удалось добавить фильм: {film_id}") from e

    async def bulk_add_films(self, films: List[Dict]) -> None:
        """
        Добавить несколько фильмов в Elasticsearch одновременно
        (bulk-операция).
        """
        logger.info("Массовое добавление фильмов. Количество: %d", len(films))

        # Разбиваем фильмы на части, чтобы избежать переполнения памяти или
        # превышения лимита запросов
        for chunk in chunked(films, BULK_CHUNK_SIZE):
            elastic_actions = [
                {
                    "_index": settings.ELASTIC_INDEX,
                    "_id": film["id"],
                    "_source": convert_decimals(film),
                }
                for film in chunk
            ]

            try:
                success, errors = await helpers.async_bulk(
                    self.elastic.es_client, elastic_actions
                )
                logger.info("Добавлено %d фильмов из текущей партии.", success)
                if errors:
                    logger.warning(
                        "Некоторые фильмы не были добавлены: %s", errors
                    )
            except ConnectionError as e:
                logger.error(
                    "Ошибка подключения к Elasticsearch при массовом "
                    "добавлении: %s",
                    e,
                )
                raise
            except RequestError as e:
                logger.error(
                    "Ошибка запроса к Elasticsearch при массовом добавлении: "
                    "%s",
                    e.info,
                )
                raise
            except Exception as e:
                logger.error("Ошибка при массовом добавлении фильмов: %s", e)
                raise RuntimeError(
                    "Не удалось выполнить массовое добавление фильмов."
                ) from e
