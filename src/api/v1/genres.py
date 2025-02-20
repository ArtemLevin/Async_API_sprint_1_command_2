from fastapi import APIRouter, HTTPException, Depends
from uuid import UUID

from src.services.genre_service import GenreService
from src.models.models import Genre
from src.db.elastic import get_elastic
from src.db.redis import get_redis

router = APIRouter()

# Создаём экземпляр GenreService
genre_service = GenreService(get_elastic, get_redis, index_name="genres")


@router.get("/{genre_uuid}", response_model=Genre)
async def get_genre(genre_uuid: UUID):
    """
    Получение жанра по UUID.
    """
    genre = await genre_service.get_by_uuid(genre_uuid)
    if not genre:
        raise HTTPException(status_code=404, detail="Жанр не найден")
    return genre


@router.get("/", response_model=list[Genre])
async def get_all_genres():
    """
    Получение списка всех жанров.
    """
    genres = await genre_service.get_all_genres()
    return genres