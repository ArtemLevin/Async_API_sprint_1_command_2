import logging

from elasticsearch import AsyncElasticsearch, helpers
from elasticsearch.exceptions import ConnectionError, RequestError
from tenacity import (retry, retry_if_exception_type, stop_after_attempt,
                      wait_exponential)

logger = logging.getLogger(__name__)


class ETLService:
    """
    Сервис для ETL-процесса, который извлекает данные жанров из фильмов и
    загружает их в отдельный индекс.
    """

    def __init__(self, elastic: AsyncElasticsearch):
        self.elastic = elastic

    # Настройки повторных попыток:
    # - Максимум 5 попыток
    # - Экспоненциальная задержка: 2, 4, 8, 10... секунд (но не более 10)
    # - Повторяем только при исключениях ConnectionError и RequestError
    # - После исчерпания попыток исключение пробрасывается дальше
    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((ConnectionError, RequestError)),
        reraise=True,
    )
    async def extract_genres_from_films(self, films_index: str) -> set:
        """
        Извлечь уникальные жанры из индекса фильмов.

        :param films_index: Имя индекса фильмов в Elasticsearch.
        :return: Множество уникальных жанров.
        """
        logger.info(f"Извлечение жанров из индекса {films_index}.")
        genres = set()

        try:
            # Используем Scroll API для извлечения всех документов из индекса `films`
            async for doc in helpers.async_scan(self.elastic, index=films_index):
                film_genres = doc["_source"].get("genre", [])
                for genre in film_genres:
                    if isinstance(genre, dict) and "id" in genre and "name" in genre:
                        genres.add((genre["id"], genre["name"]))
                    else:
                        logger.warning(f"Неверный формат жанра: {genre}")
        except ConnectionError:
            logger.error("Ошибка подключения к Elasticsearch. Попробуем снова...")
            raise
        except Exception as e:
            logger.error(f"Произошла ошибка при извлечении жанров: {e}")
            raise

        logger.info(f"Извлечено {len(genres)} уникальных жанров.")
        return genres

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((ConnectionError, RequestError)),
        reraise=True,
    )
    async def recreate_genres_index(self, genres_index: str):
        """
        Удаляет существующий индекс `genres` (если он существует) и создаёт новый с указанным маппингом.

        :param genres_index: Имя индекса жанров.
        """
        logger.info(f"Проверка существования индекса {genres_index}.")
        try:
            exists = await self.elastic.indices.exists(index=genres_index)

            if exists:
                logger.info(f"Индекс {genres_index} уже существует. Удаление индекса...")
                await self.elastic.indices.delete(index=genres_index)
                logger.info(f"Индекс {genres_index} успешно удалён.")

            logger.info(f"Создание нового индекса {genres_index} с маппингом...")
            # Определяем схему индекса
            body = {
                "mappings": {
                    "properties": {
                        "id": {"type": "keyword"},
                        "name": {"type": "text", "analyzer": "standard"},
                    }
                }
            }
            await self.elastic.indices.create(index=genres_index, body=body)
            logger.info(f"Индекс {genres_index} успешно создан.")
        except RequestError as e:
            logger.error(f"Ошибка при создании индекса {genres_index}: {e}")
            raise
        except ConnectionError:
            logger.error("Ошибка подключения к Elasticsearch. Попробуем снова...")
            raise
        except Exception as e:
            logger.error(f"Произошла ошибка при проверке/создании индекса: {e}")
            raise

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((ConnectionError, RequestError)),
        reraise=True,
    )
    async def load_genres_to_index(self, genres_index: str, genres: set):
        """
        Загружает уникальные жанры в индекс `genres`.

        :param genres_index: Имя индекса жанров.
        :param genres: Множество уникальных жанров.
        """
        logger.info(f"Загрузка жанров в индекс {genres_index}.")
        actions = [
            {
                "_index": genres_index,
                "_id": genre_id,
                "_source": {"id": genre_id, "name": genre_name},
            }
            for genre_id, genre_name in genres
        ]

        try:
            # Используем Bulk API для загрузки данных
            await helpers.async_bulk(self.elastic, actions)
            logger.info(f"Загрузка завершена. Загружено {len(genres)} жанров.")
        except ConnectionError:
            logger.error("Ошибка подключения при загрузке данных в Elasticsearch. Попробуем снова...")
            raise
        except Exception as e:
            logger.error(f"Произошла ошибка при загрузке данных: {e}")
            raise

    async def run_etl(self, films_index: str, genres_index: str):
        """
        Запускает полный ETL процесс.

        :param films_index: Имя индекса фильмов.
        :param genres_index: Имя индекса жанров.
        """
        logger.info("Запуск ETL процесса.")
        try:
            genres = await self.extract_genres_from_films(films_index)
            await self.recreate_genres_index(genres_index)
            await self.load_genres_to_index(genres_index, genres)
            logger.info("ETL процесс завершён.")
        except Exception as e:
            logger.error(f"ETL процесс завершился с ошибкой: {e}")
            raise