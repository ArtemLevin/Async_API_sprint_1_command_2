from elasticsearch import AsyncElasticsearch
from utils.decorators import with_retry


class ElasticService:
    def __init__(self, es_client: AsyncElasticsearch):
        self.es_client = es_client

    @with_retry()
    async def search(self, index: str, query: dict) -> dict:
        return await self.es_client.search(index=index, body=query)