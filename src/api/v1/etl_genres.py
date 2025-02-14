from fastapi import APIRouter, Depends, HTTPException
from http import HTTPStatus
from elasticsearch import AsyncElasticsearch
from src.services.etl_genres import ETLService
from src.db.elastic import get_elastic

router = APIRouter()

@router.post("/genres", status_code=HTTPStatus.OK)
async def run_etl(
    films_index: str = "films",
    genres_index: str = "genres",
    elastic: AsyncElasticsearch = Depends(get_elastic)
):
    """
    Запускает ETL процесс для извлечения жанров из фильмов и создания индекса жанров.

    :param films_index: Имя индекса фильмов в Elasticsearch (по умолчанию 'films').
    :param genres_index: Имя индекса жанров в Elasticsearch (по умолчанию 'genres').
    :param elastic: Экземпляр клиента Elasticsearch.
    """
    etl_service = ETLService(elastic)
    try:
        await etl_service.run_etl(films_index=films_index, genres_index=genres_index)
        return {"message": "ETL процесс успешно завершён."}
    except Exception as e:
        raise HTTPException(
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            detail=f"Ошибка при выполнении ETL процесса: {str(e)}"
        )