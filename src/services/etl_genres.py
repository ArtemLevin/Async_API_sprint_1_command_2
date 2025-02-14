import logging
from elasticsearch import AsyncElasticsearch, helpers

logger = logging.getLogger(__name__)

class ETLService:
    """
    Сервис для ETL-процесса, который извлекает данные жанров из фильмов и загружает их в отдельный индекс.
    """

    def __init__(self, elastic: AsyncElasticsearch):
        self.elastic = elastic

    async def extract_genres_from_films(self, films_index: str) -> set:
        """
        Извлечь уникальные жанры из индекса фильмов.

        :param films_index: Имя индекса фильмов в Elasticsearch.
        :return: Множество уникальных жанров.
        """
        logger.info(f"Извлечение жанров из индекса {films_index}.")
        genres = set()

        # Используем Scroll API для извлечения всех документов из индекса `films`
        async for doc in helpers.async_scan(self.elastic, index=films_index):
            film_genres = doc["_source"].get("genres", [])
            for genre in film_genres:
                genres.add((genre["id"], genre["name"]))

        logger.info(f"Извлечено {len(genres)} уникальных жанров.")
        return genres

    async def create_genres_index(self, genres_index: str):
        """
        Создаёт индекс `genres` в Elasticsearch, если он не существует.

        :param genres_index: Имя индекса жанров.
        """
        logger.info(f"Проверка существования индекса {genres_index}.")
        exists = await self.elastic.indices.exists(index=genres_index)

        if not exists:
            logger.info(f"Индекс {genres_index} не существует. Создание индекса.")
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
        else:
            logger.info(f"Индекс {genres_index} уже существует.")

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

        # Используем Bulk API для загрузки данных
        await helpers.async_bulk(self.elastic, actions)
        logger.info(f"Загрузка завершена. Загружено {len(genres)} жанров.")

    async def run_etl(self, films_index: str, genres_index: str):
        """
        Запускает полный ETL процесс.

        :param films_index: Имя индекса фильмов.
        :param genres_index: Имя индекса жанров.
        """
        logger.info("Запуск ETL процесса.")
        genres = await self.extract_genres_from_films(films_index)
        await self.create_genres_index(genres_index)
        await self.load_genres_to_index(genres_index, genres)
        logger.info("ETL процесс завершён.")