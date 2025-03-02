import logging
from typing import Tuple, Set


from elasticsearch import AsyncElasticsearch, helpers
from elasticsearch.exceptions import ConnectionError, RequestError
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)


class ETLPersonService:
    """
    Сервис для ETL-процесса, который извлекает данные о персонах из фильмов и
    загружает их в отдельный индекс.
    """

    def __init__(self, elastic: AsyncElasticsearch):
        self.elastic = elastic

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((ConnectionError, RequestError)),
        reraise=True,
    )
    async def extract_persons_from_films(self, films_index: str) -> Set[Tuple[str, str, str]]:
        """
        Извлекает уникальные данные о персонах из индекса фильмов.

        :param films_index: Имя индекса фильмов в Elasticsearch.
        :return: Множество кортежей (id, name, role).
        """
        logger.info(f"Извлечение данных о персонах из индекса {films_index}.")
        persons = set()

        try:
            # Используем Scroll API для извлечения всех документов из индекса `films`
            async for doc in helpers.async_scan(self.elastic, index=films_index):
                film_id = doc["_id"]
                actors = doc["_source"].get("actors", [])
                writers = doc["_source"].get("writers", [])
                directors = doc["_source"].get("directors", [])

                for actor in actors:
                    if isinstance(actor, dict) and "uuid" in actor and "full_name" in actor:
                        persons.add((actor["uuid"], actor["full_name"], "actor"))
                    else:
                        logger.warning(f"Неверный формат актера: {actor}")

                for writer in writers:
                    if isinstance(writer, dict) and "uuid" in writer and "full_name" in writer:
                        persons.add((writer["uuid"], writer["full_name"], "writer"))
                    else:
                        logger.warning(f"Неверный формат сценариста: {writer}")

                for director in directors:
                    if isinstance(director, dict) and "uuid" in director and "full_name" in director:
                        persons.add((director["uuid"], director["full_name"], "director"))
                    else:
                        logger.warning(f"Неверный формат режиссера: {director}")

        except ConnectionError:
            logger.error("Ошибка подключения к Elasticsearch. Попробуем снова...")
            raise
        except Exception as e:
            logger.error(f"Произошла ошибка при извлечении данных о персонах: {e}")
            raise

        logger.info(f"Извлечено {len(persons)} уникальных персон.")
        return persons

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((ConnectionError, RequestError)),
        reraise=True,
    )
    async def recreate_person_index(self, person_index: str):
        """
        Удаляет существующий индекс `persons` (если он существует) и создаёт новый с указанным маппингом.

        :param person_index: Имя индекса персон.
        """
        logger.info(f"Проверка существования индекса {person_index}.")
        try:
            exists = await self.elastic.indices.exists(index=person_index)

            if exists:
                logger.info(f"Индекс {person_index} уже существует. Удаление индекса...")
                await self.elastic.indices.delete(index=person_index)
                logger.info(f"Индекс {person_index} успешно удалён.")

            logger.info(f"Создание нового индекса {person_index} с маппингом...")
            # Определяем схему индекса
            body = {
                "mappings": {
                    "properties": {
                        "id": {"type": "keyword"},
                        "name": {"type": "text", "analyzer": "standard"},
                        "role": {"type": "keyword"},
                        "films": {"type": "keyword"},
                    }
                }
            }
            await self.elastic.indices.create(index=person_index, body=body)
            logger.info(f"Индекс {person_index} успешно создан.")
        except RequestError as e:
            logger.error(f"Ошибка при создании индекса {person_index}: {e}")
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
    async def load_persons_to_index(self, person_index: str, persons: Set[Tuple[str, str, str]]):
        """
        Загружает уникальные данные о персонах в индекс `persons`.

        :param person_index: Имя индекса персон.
        :param persons: Множество кортежей (id, name, role).
        """
        logger.info(f"Загрузка данных о персонах в индекс {person_index}.")
        actions = [
            {
                "_index": person_index,
                "_id": person_id,
                "_source": {
                    "id": person_id,
                    "name": person_name,
                    "role": person_role,
                    "films": [],  # Пустой список фильмов (можно дополнить позже)
                },
            }
            for person_id, person_name, person_role in persons
        ]

        try:
            # Используем Bulk API для загрузки данных
            await helpers.async_bulk(self.elastic, actions)
            logger.info(f"Загрузка завершена. Загружено {len(persons)} персон.")
        except ConnectionError:
            logger.error("Ошибка подключения при загрузке данных в Elasticsearch. Попробуем снова...")
            raise
        except Exception as e:
            logger.error(f"Произошла ошибка при загрузке данных: {e}")
            raise

    async def run_etl(self, films_index: str, person_index: str):
        """
        Запускает полный ETL процесс.

        :param films_index: Имя индекса фильмов.
        :param person_index: Имя индекса персон.
        """
        logger.info("Запуск ETL процесса.")
        try:
            persons = await self.extract_persons_from_films(films_index)
            await self.recreate_person_index(person_index)
            await self.load_persons_to_index(person_index, persons)
            logger.info("ETL процесс завершён.")
        except Exception as e:
            logger.error(f"ETL процесс завершился с ошибкой: {e}")
            raise