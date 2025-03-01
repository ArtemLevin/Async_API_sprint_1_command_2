import logging
from http import HTTPStatus
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query

from src.services.film import FilmService, get_film_service


logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/{film_id}")
async def film_details(
    film_id: UUID,
    film_service: FilmService = Depends(get_film_service),
) -> dict:
    """
    Эндпоинт для получения информации о фильме по его ID.
    """
    film_dump = await film_service.get_film_by_id(str(film_id))

    if not film_dump:
        # Выбрасываем HTTP-исключение с кодом 404
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail="Film not found",
        )

    return film_dump


@router.get("/search")
async def search_films(
    query: str = Query(..., description="Поисковый запрос по фильмам"),
    page_size: int = Query(
        10,
        ge=1,
        le=100,
        description="Количество фильмов в результате (от 1 до 100)",
    ),
    page_number: int = Query(
        1,
        ge=1,
        description="Смещение для пагинации (неотрицательное число)",
    ),
    film_service: FilmService = Depends(get_film_service),
) -> list[dict] | None:
    """
    Эндпоинт для получения фильмов с поддержкой сортировки по рейтингу,
    фильтрации по жанру и пагинацией.
    """
    films_dump = await film_service.search_films(
        query=query, page_size=page_size, page_number=page_number
    )

    if not films_dump:
        # Выбрасываем HTTP-исключение с кодом 404
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail="Films not found",
        )

    return films_dump


@router.get("")
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
            description="Количество фильмов в результате (от 1 до 100)",
        ),
        page_number: int = Query(
            1,
            ge=1,
            description="Смещение для пагинации (неотрицательное число)",
        ),
        film_service: FilmService = Depends(get_film_service),
) -> list[dict] | None:
    """
    Эндпоинт для получения фильмов с поддержкой сортировки по рейтингу,
    фильтрации по жанру и пагинацией.
    """
    films_dump = await film_service.get_films(
        sort=sort,
        genre=str(genre),
        page_size=page_size,
        page_number=page_number,
    )

    if not films_dump:
        # Выбрасываем HTTP-исключение с кодом 404
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail="Films not found",
        )

    return films_dump
