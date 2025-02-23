from typing import Optional

from elasticsearch import AsyncElasticsearch

from src.utils.elastic_service import ElasticService

es: Optional[AsyncElasticsearch] = None


async def get_elastic() -> AsyncElasticsearch:
    global es
    if not es:  # Создаём соединение, если его ещё нет
        es = AsyncElasticsearch(hosts=["http://localhost:9200"])
    return ElasticService(es)
