from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException

from src.db.elastic import get_elastic
from src.db.redis_client import get_redis
from src.models.models import Genre
from src.services.genre_service import GenreService

router = APIRouter()

async def get_genre_service(
    elastic=Depends(get_elastic), redis=Depends(get_redis)
) -> GenreService:
    """
    Фабрика для создания экземпляра GenreService с зависимостями.
    """
    return GenreService(elastic, redis, index_name="genres")


@router.get("/{genre_uuid}", response_model=Genre)
async def get_genre(
    genre_uuid: UUID, genre_service: GenreService = Depends(get_genre_service)
):
    """
    Получение жанра по UUID.
    """
    genre = await genre_service.get_by_uuid(genre_uuid)
    if not genre:
        raise HTTPException(status_code=404, detail="Жанр не найден")
    return genre


@router.get("/", response_model=list[Genre])
async def get_all_genres(
    genre_service: GenreService = Depends(get_genre_service)
):
    """
    Получение списка всех жанров.
    """
    genres = await genre_service.get_all_genres()
    return genres