class BaseServiceError(Exception):
    """Базовое исключение для всех ошибок сервиса."""
    pass


class CacheServiceError(BaseServiceError):
    """Исключение для ошибок, связанных с кэшем."""
    pass


class ElasticServiceError(BaseServiceError):
    """Исключение для ошибок, связанных с Elasticsearch."""
    pass


class CreateObjectError(BaseServiceError):
    """Исключение для ошибок, связанных с созданием объекта модели."""
    pass


class ElasticParsingError(BaseServiceError):
    """Исключение для ошибок, связанных с парсингом данных из Elasticsearch."""
    pass


class CreateObjectsError(BaseServiceError):
    """Исключение для ошибок, связанных с созданием списка объектов модели."""
    pass


class ModelDumpError(BaseServiceError):
    """
    Исключение для ошибок, связанных с сериализацией объекта модели в словарь.
    """
    pass


class ModelDumpJsonError(BaseServiceError):
    """
    Исключение для ошибок, связанных с созданием json из объекта(ов) модели.
    """
    pass


class JsonLoadsError(BaseServiceError):
    """
    Исключение для ошибок, связанных с десериализацией json в объект
    Python.
    """
    pass


class CheckCacheError(BaseServiceError):
    """Исключение для ошибок, связанных с проверкой записи в кеше."""
    pass


class CheckElasticError(BaseServiceError):
    """Исключение для ошибок, связанных с проверкой записи в elastic."""
    pass


class ElasticNotFoundError(BaseServiceError):
    """Исключение для ошибок, связанных с отсутствием записи в elastic."""
    pass
