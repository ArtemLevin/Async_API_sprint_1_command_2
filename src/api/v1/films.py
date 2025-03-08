import logging
from http import HTTPStatus
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from src.models.models import Film, FilmBase
from src.services.film_service import FilmService, get_film_service

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/search", response_model=list[FilmBase])
async def search_films(
    query: str | None = Query(None, description="Поисковый запрос по фильмам"),
    page_size: int = Query(
        10,
        ge=1,
        le=100,
        description="Количество записей в результате (от 1 до 100)",
    ),
    page_number: int = Query(
        1,
        ge=1,
        description="Смещение для пагинации (больше ноля)",
    ),
    film_service: FilmService = Depends(get_film_service),
) -> list[BaseModel | None]:
    """
    Эндпоинт для поиска фильмов с поддержкой поиска по названию
    и пагинацией.
    """
    films = await film_service.search_films(
        query=query, page_size=page_size, page_number=page_number
    )
    if not films:
        # Выбрасываем HTTP-исключение с кодом 404
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail="Films not found",
        )

    return films


@router.get("", response_model=list[FilmBase])
async def get_films(
        genre: UUID | None = Query(
            None, description="UUID жанра для фильтрации"
        ),
        sort: str = Query(
            "-imdb_rating",
            description="Поле для сортировки (например, '-imdb_rating')",
        ),
        page_size: int = Query(
            10,
            ge=1,
            le=100,
            description="Количество записей в результате (от 1 до 100)",
        ),
        page_number: int = Query(
            1,
            ge=1,
            description="Смещение для пагинации (больше ноля)",
        ),
        film_service: FilmService = Depends(get_film_service),
) -> list[BaseModel | None]:
    """
    Эндпоинт для получения фильмов с поддержкой сортировки по рейтингу,
    фильтрации по жанру и пагинацией.
    """
    films = await film_service.get_films(
        sort=sort,
        genre=genre,
        page_size=page_size,
        page_number=page_number,
    )

    if not films:
        # Выбрасываем HTTP-исключение с кодом 404
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail="Films not found",
        )

    return films


@router.get("/{film_id}", response_model=Film)
async def film_details(
    film_id: UUID,
    film_service: FilmService = Depends(get_film_service),
) -> BaseModel:
    """Эндпоинт для получения фильма по ID."""
    film_id = str(film_id)
    film = await film_service.get_film_by_id(film_id)

    if not film:
        logger.info("Фильм с ID: %s не найден.", film_id)
        # Выбрасываем HTTP-исключение с кодом 404
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail="Film not found",
        )

    return film
