class BaseServiceError(Exception):
    """Базовое исключение для всех ошибок сервиса."""
    pass


class CacheServiceError(BaseServiceError):
    """Исключение для ошибок, связанных с кэшем."""
    pass


class ElasticServiceError(BaseServiceError):
    """Исключение для ошибок, связанных с Elasticsearch."""
    pass