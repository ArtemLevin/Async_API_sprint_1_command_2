from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk
from typing import List, AsyncIterator
import logging
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from logging.config import dictConfig
from core.logger import LOGGING

dictConfig(LOGGING)

logger = logging.getLogger(__name__)


class ETLPersonService:
    def __init__(self, elastic: AsyncElasticsearch):
        """
        Инициализация ETLPersonService.

        :param elastic: Экземпляр клиента Elasticsearch для взаимодействия с индексами.
        """
        self.elastic = elastic

    @retry(
        stop=stop_after_attempt(5),  # Останавливаемся после 5 попыток
        wait=wait_exponential(multiplier=1, min=2, max=10),  # Экспоненциальная задержка: 2, 4, 8, 10...
        retry=retry_if_exception_type(Exception),  # Повторяем только при исключениях
        reraise=True  # Пробрасываем исключение после исчерпания попыток
    )
    async def create_person_index(self, person_index: str) -> None:
        """
        Создаёт индекс для персон с заданной схемой, если он ещё не существует.

        :param person_index: Имя индекса для хранения данных о персонах.
        """
        logger.info(f"Проверяем существование индекса '{person_index}'...")
        mapping = {
            "mappings": {
                "properties": {
                    "id": {"type": "keyword"},
                    "name": {"type": "text", "analyzer": "standard"},
                    "role": {"type": "text", "analyzer": "standard"},
                    "films": {"type": "keyword"}
                }
            }
        }
        index_exists = await self.elastic.indices.exists(index=person_index)
        if not index_exists:
            logger.info(f"Создаём индекс '{person_index}'...")
            await self.elastic.indices.create(index=person_index, body=mapping)
            logger.info(f"Индекс '{person_index}' успешно создан.")
        else:
            logger.info(f"Индекс '{person_index}' уже существует, пропускаем создание.")

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(Exception),
        reraise=True
    )
    async def extract_persons(self, films_index: str) -> AsyncIterator[dict]:
        """
        Извлекает данные о персон из индекса фильмов.

        :param films_index: Имя индекса фильмов, из которого извлекаются данные.
        :yield: Генератор словарей с информацией о персонах.
        """
        logger.info(f"Начинаем извлечение данных о персонах из индекса '{films_index}'...")
        query = {"query": {"match_all": {}}}
        try:
            async for doc in self.elastic.helpers.scan(self.elastic, index=films_index, query=query):
                film_id = doc["_id"]
                persons = doc["_source"].get("persons", [])
                for person in persons:
                    yield {
                        "id": person["id"],
                        "name": person["name"],
                        "role": person["role"],
                        "films": [film_id]
                    }
        except Exception as e:
            logger.error(f"Ошибка при извлечении данных из индекса '{films_index}': {e}")
            raise

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(Exception),
        reraise=True
    )
    async def load_persons(self, person_index: str, persons: List[dict]) -> None:
        """
        Загружает данные о персонах в индекс Elasticsearch.

        :param person_index: Имя индекса для загрузки данных.
        :param persons: Список документов о персонах.
        """
        logger.info(f"Начинаем загрузку данных в индекс '{person_index}'...")
        actions = [
            {
                "_index": person_index,
                "_id": person["id"],
                "_source": person
            }
            for person in persons
        ]
        try:
            success, failed = await async_bulk(self.elastic, actions)
            logger.info(f"Загрузка завершена: Успешно - {success}, Неудачно - {failed}")
        except Exception as e:
            logger.error(f"Ошибка при загрузке данных в индекс '{person_index}': {e}")
            raise

    async def run_etl(self, films_index: str, person_index: str) -> None:
        """
        Запускает полный процесс ETL для создания индекса персон.

        :param films_index: Имя индекса фильмов, из которого извлекаются данные.
        :param person_index: Имя индекса для хранения данных о персонах.
        """
        logger.info("Запуск ETL процесса...")
        try:
            # Шаг 1: Создание индекса для персон
            await self.create_person_index(person_index)

            # Шаг 2: Извлечение данных о персонах
            logger.info("Извлечение данных о персонах...")
            persons = []
            async for person in self.extract_persons(films_index):
                persons.append(person)
            logger.info(f"Извлечено {len(persons)} персон.")

            # Шаг 3: Загрузка данных в индекс
            logger.info("Загрузка данных в Elasticsearch...")
            await self.load_persons(person_index, persons)

            logger.info("ETL процесс успешно завершён.")
        except Exception as e:
            logger.error(f"ETL процесс завершился с ошибкой: {e}")
            raise