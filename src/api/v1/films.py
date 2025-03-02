from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException

from src.db.elastic import get_elastic
from src.db.redis_client import get_redis
from src.models.models import Film
from src.services.film_service import FilmService

router = APIRouter()


async def get_film_service(
        elastic=Depends(get_elastic), redis=Depends(get_redis)
) -> FilmService:
    return FilmService(elastic, redis, index_name="films")


@router.get("/{film_uuid}", response_model=Film)
async def get_film(
        film_uuid: UUID, film_service: FilmService = Depends(get_film_service)
):
    """
    Эндпоинт для получения информации о фильме по UUID.
    """
    film = await film_service.get_by_uuid(film_uuid)
    if not film:
        raise HTTPException(status_code=404, detail="Фильм не найден")
    return film


@router.get("/", response_model=list[Film])
async def get_all_films(
        film_service: FilmService = Depends(get_film_service)
):
    """
    Получение списка всех фильмов.
    """
    films = await film_service.get_all_films()
    return films
