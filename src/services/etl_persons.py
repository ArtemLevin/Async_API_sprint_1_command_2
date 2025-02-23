import logging
from logging.config import dictConfig
from typing import AsyncIterator, List

from elasticsearch import AsyncElasticsearch, helpers
from elasticsearch._async.helpers import async_bulk
from elasticsearch.exceptions import (ConnectionError, NotFoundError,
                                      RequestError)
from tenacity import (retry, retry_if_exception_type, stop_after_attempt,
                      wait_exponential)

from src.core.logger import LOGGING

dictConfig(LOGGING)

logger = logging.getLogger(__name__)


class ETLPersonService:
    def __init__(self, elastic: AsyncElasticsearch):
        """
        Инициализация ETLPersonService.

        :param elastic: Экземпляр клиента Elasticsearch для взаимодействия с
        индексами.
        """
        self.elastic = elastic

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((ConnectionError, RequestError)),
        reraise=True
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
        try:
            index_exists = await self.elastic.indices.exists(
                index=person_index
            )
            if not index_exists:
                logger.info(
                    f"Создаём индекс '{person_index}' с мэппингом: {mapping}"
                )
                await self.elastic.indices.create(
                    index=person_index, body=mapping
                )
                logger.info(f"Индекс '{person_index}' успешно создан.")
            else:
                logger.info(
                    f"Индекс '{person_index}' уже существует, пропускаем "
                    f"создание."
                )
        except NotFoundError as e:
            logger.error(
                f"Индекс '{person_index}' не найден. Исключение: {e}",
                extra={"index": person_index}
            )
            raise
        except RequestError as e:
            logger.error(
                f"Ошибка запроса при создании индекса '{person_index}'. "
                f"Исключение: {e}",
                extra={"index": person_index, "mapping": mapping}
            )
            raise
        except ConnectionError as e:
            logger.error(
                f"Ошибка подключения при создании индекса '{person_index}'. "
                f"Исключение: {e}",
                extra={"index": person_index}
            )
            raise
        except Exception as e:
            logger.error(
                f"Произошла непредвиденная ошибка при создании индекса "
                f"'{person_index}'. Исключение: {e}",
                extra={"index": person_index}
            )
            raise

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((ConnectionError, RequestError)),
        reraise=True
    )
    async def extract_persons(self, films_index: str) -> AsyncIterator[dict]:
        """
        Извлекает данные из индекса фильмов.

        :param films_index: Имя индекса фильмов, из которого извлекаются
        данные.
        :yield: Генератор словарей с информацией о персонах.
        """
        logger.info(
            f"Начинаем извлечение данных о персонах из индекса "
            f"'{films_index}'..."
        )
        query = {"query": {"match_all": {}}}

        try:
            async for doc in helpers.async_scan(
                    self.elastic, index=films_index, query=query
            ):
                film_id = doc["_id"]
                persons = doc["_source"].get("persons", [])
                logger.debug(
                    f"Обрабатывается документ с ID '{film_id}': найдено "
                    f"{len(persons)} персон.",
                    extra={"film_id": film_id, "persons_count": len(persons)}
                )
                for person in persons:
                    yield {
                        "id": person["id"],
                        "name": person["name"],
                        "role": person["role"],
                        "films": [film_id]
                    }
        except NotFoundError as e:
            logger.error(
                f"Индекс '{films_index}' не найден. Исключение: {e}",
                extra={"index": films_index, "query": query}
            )
            raise
        except ConnectionError as e:
            logger.error(
                f"Ошибка подключения при извлечении данных из индекса "
                f"'{films_index}'. Исключение: {e}",
                extra={"index": films_index}
            )
            raise
        except Exception as e:
            logger.error(
                f"Произошла непредвиденная ошибка при извлечении данных из "
                f"индекса '{films_index}'. Исключение: {e}",
                extra={"index": films_index, "query": query}
            )
            raise

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((ConnectionError, RequestError)),
        reraise=True
    )
    async def load_persons(
            self, person_index: str, persons: List[dict]
    ) -> None:
        """
        Загружает данные о персонах в индекс Elasticsearch.

        :param person_index: Имя индекса для загрузки данных.
        :param persons: Список документов о персонах.
        """
        logger.info(f"Начинаем загрузку данных в индекс '{person_index}'.")
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
            logger.info(
                f"Загрузка завершена. Результаты: Успешно - {success}, "
                f"Неудачно - {failed}",
                extra={
                    "success_count": success,
                    "failed_count": failed,
                    "index": person_index
                }
            )
        except ConnectionError as e:
            logger.error(
                f"Ошибка подключения при загрузке данных в индекс "
                f"'{person_index}'. Исключение: {e}",
                extra={"index": person_index, "actions_count": len(actions)}
            )
            raise
        except RequestError as e:
            logger.error(
                f"Ошибка запроса при загрузке данных в индекс "
                f"'{person_index}'. Исключение: {e}",
                extra={"index": person_index, "actions_count": len(actions)}
            )
            raise
        except Exception as e:
            logger.error(
                f"Произошла непредвиденная ошибка при загрузке данных в "
                f"индекс '{person_index}'. Исключение: {e}",
                extra={"index": person_index, "actions_count": len(actions)}
            )
            raise

    async def run_etl(self, films_index: str, person_index: str) -> None:
        """
        Запускает полный процесс ETL для создания индекса персон.

        :param films_index: Имя индекса фильмов, из которого извлекаются
        данные.
        :param person_index: Имя индекса для хранения данных о персонах.
        """
        logger.info("Запуск ETL процесса...")
        try:
            await self.create_person_index(person_index)

            logger.info(
                f"Извлечение данных о персонах из индекса '{films_index}'."
            )
            persons = []
            async for person in self.extract_persons(films_index):
                persons.append(person)
            logger.info(
                f"Извлечение данных завершено: извлечено {len(persons)} "
                f"персон.",
                extra={
                    "films_index": films_index,
                    "persons_count": len(persons)
                }
            )

            logger.info(f"Загрузка данных в индекс '{person_index}'.")
            await self.load_persons(person_index, persons)

            logger.info("ETL процесс успешно завершён.")
        except Exception as e:
            logger.error(
                f"ETL процесс завершился с ошибкой: {e}",
                extra={
                    "films_index": films_index,
                    "person_index": person_index
                }
            )
            raise
