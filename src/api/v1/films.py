import logging
from http import HTTPStatus
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query

from src.services.film import FilmService, get_film_service

# Настраиваем логгер
logger = logging.getLogger(__name__)

# Создаём маршрут для работы с фильмами
router = APIRouter()


@router.get("/{film_id}")
async def film_details(
    film_id: UUID,
    film_service: FilmService = Depends(get_film_service)
) -> dict:
    """
    Эндпоинт для получения информации о фильме по его ID.

    :param film_id: Уникальный идентификатор фильма.
    :param film_service: Сервис для работы с данными фильмов (внедряется
    через Depends).

    :return: Словарь с данными по фильму.

    :raises HTTPException: Если фильм не найден, возвращается статус 404.
    """
    film_dump = await film_service.get_film_by_id(str(film_id))

    if not film_dump:
        # Выбрасываем HTTP-исключение с кодом 404
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail="Film not found"
        )

    return film_dump


@router.get("/search")
async def search_films(
    query: str = Query(..., description="Поисковый запрос по фильмам"),
    limit: int = Query(
        10,
        ge=1,
        le=100,
        description="Количество фильмов в результате (от 1 до 100)",
    ),
    offset: int = Query(
        0,
        ge=0,
        description="Смещение для пагинации (неотрицательное число)",
    ),
    film_service: FilmService = Depends(get_film_service),
) -> list[dict] | None:
    """
    Эндпоинт для получения фильмов с поддержкой сортировки по рейтингу,
    фильтрации по жанру и пагинацией.

    :param query: Поисковый запрос (строка, обязательный).
    :param limit: Максимальное количество фильмов в одном запросе (по
    умолчанию 10).
    :param offset: Смещение для пагинации (по умолчанию 0).
    :param film_service: Сервис для работы с фильмами.
    :return: Список со словарями содержащими данные о фильме, отсортированных
    и отфильтрованных по указанным параметрам.
    """
    films_dump = await film_service.search_films(
        query=query, limit=limit, offset=offset
    )

    if not films_dump:
        # Выбрасываем HTTP-исключение с кодом 404
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail="Films not found"
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
        limit: int = Query(
            10,
            ge=1,
            le=100,
            description="Количество фильмов в результате (от 1 до 100)",
        ),
        offset: int = Query(
            0,
            ge=0,
            description="Смещение для пагинации (неотрицательное число)",
        ),
        film_service: FilmService = Depends(get_film_service),
) -> list[dict] | None:
    """
    Эндпоинт для получения фильмов с поддержкой сортировки по рейтингу,
    фильтрации по жанру и пагинацией.

    :param genre: UUID жанра для фильтрации (пример: <comedy-uuid>).
    :param sort: Поле для сортировки (пример: "-imdb_rating" для убывания).
    :param limit: Максимальное количество фильмов в одном запросе (по
    умолчанию 10).
    :param offset: Смещение для пагинации (по умолчанию 0).
    :param film_service: Сервис для работы с фильмами.
    :return: Список со словарями содержащими данные о фильме, отсортированных
    и отфильтрованных по указанным параметрам.
    """
    films_dump = await film_service.get_films(
        sort=sort, genre=str(genre), limit=limit, offset=offset
    )

    if not films_dump:
        # Выбрасываем HTTP-исключение с кодом 404
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail="Films not found"
        )

    return films_dump
