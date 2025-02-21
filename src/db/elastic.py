from typing import Optional
from elasticsearch import AsyncElasticsearch
from utils.elastic_service import ElasticService
from core.config import ELASTIC_HOST, ELASTIC_PORT, ELASTIC_SCHEMA

es: Optional[AsyncElasticsearch] = None

async def get_elastic() -> ElasticService:
    global es
    if es:
        await es.close()  # Закрываем старое соединение
    es = AsyncElasticsearch(hosts=[f"{ELASTIC_SCHEMA}{ELASTIC_HOST}:{ELASTIC_PORT}"])
    return ElasticService(es)
