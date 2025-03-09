import logging
from typing import Any, Type

import orjson
from pydantic import BaseModel, ValidationError

from src.core.exceptions import (CacheServiceError, CheckCacheError,
                                 CheckElasticError, CreateObjectError,
                                 CreateObjectsError, ElasticNotFoundError,
                                 ElasticParsingError, ElasticServiceError,
                                 JsonLoadsError, ModelDumpError,
                                 ModelDumpJsonError)
from src.utils.cache_service import CacheService
from src.utils.elastic_service import ElasticService

logger = logging.getLogger(__name__)


class BaseService:
    """
    Базовый сервис.

    Осуществляет взаимодействие с Redis (для кеширования)
    и Elasticsearch (для полнотекстового поиска).
    """

    def __init__(self, redis_client: CacheService, es_client: ElasticService):
        self.redis_client = redis_client
        self.es_client = es_client

    @staticmethod
    def _model_dump(
        obj: BaseModel,
        exclude: set[str] | dict | None = None,
        log_info: str = "",
    ) -> dict:
        """
        Вспомогательный метод для генерации словаря из объекта модели Pydantic.
        """
        try:
            return obj.model_dump(
                mode='json', exclude=exclude
            )

        except (AttributeError, TypeError, ValueError, KeyError) as e:
            logger.error(
                "Ошибка при сериализации объекта модели %s с ID %s в словарь: "
                "%s. %s",
                obj.id, obj.__class__.__name__, e, log_info
            )
            raise ModelDumpError(e)

    @staticmethod
    def _create_object_from_dict(
        model: Type[BaseModel], data: dict, log_info: str = ""
    ) -> BaseModel:
        """
        Вспомогательный метод для создания объекта модели Pydantic из словаря.
        """
        try:
            return model(**data)

        except ValidationError as e:
            logger.warning(
                "Ошибка при валидации из словаря в модель %s: %s. "
                "Данные для валидации: %s. %s",
                model.__name__, e, data, log_info
            )
            raise CreateObjectError(e.json())

    @staticmethod
    def _get_record_from_source(data: dict, log_info: str = "") -> dict:
        """
        Вспомогательный метод для извлечения данных по записи из Elasticsearch.
        """
        try:
            record = data["_source"]
            if isinstance(record, dict):
                return record
            raise TypeError(
                "Ожидался dict, но получен %s. %s",
                type(record).__name__, log_info
            )

        except (KeyError, TypeError) as e:
            logger.error(
                "Ошибка в структуре данных из Elasticsearch: %s"
                "Переданные данные для получения значения по ключа "
                "'_source': %s. %s",
                e, data, log_info
            )
            raise ElasticParsingError(e)

    @staticmethod
    def _get_records_from_hits(data: dict, log_info: str = "") -> list[dict]:
        """
        Вспомогательный метод для извлечения списка записей из Elasticsearch.
        """
        try:
            return data["hits"]["hits"]

        except (KeyError, TypeError) as e:
            logger.error(
                "Ошибка некорректного ответа от Elasticsearch: %s. %s",
                e, log_info
            )
            raise ElasticParsingError(e)

    @staticmethod
    def _get_data_from_json(json: bytes | None, log_info: str = "") -> Any:
        """Вспомогательный метод для десериализации JSON в объекты Python."""
        try:
            return orjson.loads(json)

        except orjson.JSONDecodeError as e:
            logger.warning(
                "Ошибка при попытки декодировать json в объект Python. "
                "json: %s. %s",
                json, log_info
            )
            raise JsonLoadsError(e)

    @staticmethod
    def _create_json_from_data(data: dict | list, log_info: str = "") -> bytes:
        try:
            return orjson.dumps(data)

        except orjson.JSONEncodeError as e:
            logger.error(
                "Ошибка сериализации словаря в json: %s. "
                "Входные данные: %s. %s",
                e, data, log_info
            )
            raise ModelDumpJsonError(e)

    def _create_objects(
        self,
        model: Type[BaseModel],
        data: list[dict],
        log_info: str = "",
    ) -> list[BaseModel]:
        """
        Вспомогательный метод для создания списка с объектами модели Pydantic.
        """
        valid_objects = []

        for cell in data:
            try:
                record = self._get_record_from_source(cell)

            except ElasticParsingError:
                pass

            else:
                try:
                    model_obj = self._create_object_from_dict(model, record)

                except CreateObjectError:
                    pass

                else:
                    valid_objects.append(model_obj)

        if not valid_objects:
            if data:
                raise CreateObjectsError(
                    "Не удалось создать ни одного объекта модели %s "
                    "из переданных данных: %s. %s",
                    model.__name__, data, log_info
                )

        return valid_objects

    def _create_json_from_objects(
        self, data: list[BaseModel], log_info: str = ""
    ) -> bytes:
        """
        Вспомогательный метод для создания json из списка объектов Pydantic.
        """
        valid_data = []

        for model_obj in data:
            try:
                model_data = self._model_dump(model_obj)

            except ModelDumpError as e:
                raise ModelDumpJsonError(e)

            else:
                valid_data.append({"_source": model_data})

        return self._create_json_from_data(valid_data, log_info)

    async def _get_from_cache(
            self, model: Type[BaseModel], cache_key: str, log_info: str = ""
    ):
        try:
            cache_json = await self.redis_client.get(cache_key, log_info)
            cache_data = self._get_data_from_json(cache_json, log_info)
            result = self._create_objects(model, cache_data, log_info)

        except (CacheServiceError, JsonLoadsError, CreateObjectsError) as e:
            raise CheckCacheError(e)

        else:
            logger.info("Данные из кеша прошли валидацию. %s", log_info)

            return result

    async def _get_record_from_elastic(
        self,
        model: Type[BaseModel],
        index: str,
        id: str,
        log_info: str = "",
    ) -> BaseModel | None:
        """Вспомогательный метод для поиска записи в Elasticsearch."""
        try:
            response = await self.es_client.get(
                index, id, log_info
            )
            record_data = self._get_record_from_source(response, log_info)
            record_obj = self._create_object_from_dict(
                model, record_data, log_info
            )

        except (
            ElasticServiceError, CreateObjectError, ElasticParsingError
        ) as e:
            raise CheckElasticError(e)

        except ElasticNotFoundError:
            logger.info(
                "Запись с ID %s не найдена в Elasticsearch. %s", id, log_info
            )
            return None

        else:
            logger.info(
                "Запись с ID %s найдена в Elasticsearch. %s", id, log_info
            )
            return record_obj

    async def _get_records_from_elastic(
        self,
        model: Type[BaseModel],
        index: str,
        body: dict,
        log_info: str = "",
    ) -> list[BaseModel]:
        """Вспомогательный метод для поиска записей в Elasticsearch."""
        try:
            response = await self.es_client.search(
                index, body, log_info)
            records_data = self._get_records_from_hits(response, log_info)
            records_obj = self._create_objects(model, records_data, log_info)

        except (
            ElasticServiceError, ElasticParsingError, CreateObjectsError
        ) as e:
            logger.warning(
                "Не удалось получить ни одного объекта модели %s из "
                "полученных данных от Elasticsearch. %s",
                model.__name__, log_info,
            )
            raise CheckElasticError(e)

        else:
            logger.info(
                "Из Elasticsearch получено записей в количестве: %d шт. %s",
                len(records_obj), log_info
            )
            return records_obj

    async def _put_to_cache(
        self, cache_key: str, data: list[BaseModel] | str, log_info: str = ""
    ) -> None:
        """Вспомогательные метод для кеширования записей."""
        try:
            json_represent = self._create_json_from_objects(
                data, log_info
            )
            await self.redis_client.set(cache_key, json_represent, log_info)

        except (CacheServiceError, ModelDumpJsonError):
            pass

    async def _get_by_id(
            self,
            model: Type[BaseModel],
            index: str,
            id: str,
            cache_key: str,
            log_info: str,
    ) -> BaseModel | None:
        """
        Вспомогательный метод для получения записи по ID. Поддерживает кеш.
        """
        # Проверяем наличие результата в кеше (Redis)
        try:
            cache = await self._get_from_cache(
                model, cache_key, log_info
            )
            return cache[0]

        except IndexError:
            return None

        except CheckCacheError:
            pass

        # Проверяем наличие результата в Elasticsearch
        try:
            obj = await self._get_record_from_elastic(
                model, index, id, log_info
            )
        except CheckElasticError:
            return None

        else:
            # Кешируем асинхронно фильм в Redis
            if obj:
                cache_data = [obj]

            else:
                cache_data = []

            await self._put_to_cache(cache_key, cache_data, log_info)

            return obj
