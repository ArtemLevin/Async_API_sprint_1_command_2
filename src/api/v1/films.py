import logging
from http import HTTPStatus
from typing import Annotated, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from src.services.film import FilmService, get_film_service
from src.models.models import Film

# Настраиваем логгер
logger = logging.getLogger(__name__)

# Создаём маршрут для работы с фильмами
router = APIRouter()


@router.get("/{film_id}", response_model=Film)
async def film_details(
    film_id: str,
    film_service: Annotated[FilmService, Depends(get_film_service)]
) -> Film:
    """
    Эндпоинт для получения информации о фильме по его ID.

    :param film_id: Уникальный идентификатор фильма.
    :param film_service: Сервис для работы с данными фильмов (внедряется через Depends).

    :return: Объект Film, содержащий ID и название фильма.

    :raises HTTPException: Если фильм не найден, возвращается статус 404.
    """
    logger.info(f"Получение информации о фильме с ID: {film_id}")

    # Получаем фильм из FilmService
    film = await film_service.get_film_by_id(film_id)

    if not film:
        # Логируем, если фильм не найден
        logger.warning(f"Фильм с ID {film_id} не найден")
        # Выбрасываем HTTP-исключение с кодом 404
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail="Film not found"
        )

    # Логируем успешное получение фильма
    logger.info(f"Фильм с ID {film_id} успешно найден: {film}")

    # Преобразуем данные из бизнес-логики в модель ответа
    return Film(id=film["id"], title=film["title"])

@router.get("/popular", response_model=List[Film])
async def get_popular_films(
    limit: int = Query(10, ge=1, le=100, description="Количество фильмов в результате (от 1 до 100)"),
    offset: int = Query(0, ge=0, description="Смещение для пагинации (неотрицательное число)"),
    film_service: FilmService = Depends(get_film_service)
) -> List[Film]:
    """
    Эндпоинт для получения популярных фильмов.

    Возвращает список фильмов, отсортированных по убыванию рейтинга IMDb.

    :param limit: Максимальное количество фильмов, возвращаемых в одном запросе (по умолчанию 10).
    :param offset: Смещение для пагинации (по умолчанию 0).
    :param film_service: Сервис для работы с фильмами, внедряется через Depends.
    :return: Список фильмов (модели Film) с полями, такими как `id`, `title`, `imdb_rating`.
    :raises HTTPException: Если возникают ошибки при обращении к сервису.
    """
    logger.info("Запрос на получение популярных фильмов: limit=%d, offset=%d", limit, offset)

    try:
        # Получаем список популярных фильмов через FilmService
        popular_films = await film_service.get_popular_films(limit=limit, offset=offset)
        logger.info("Успешно получено %d популярных фильмов", len(popular_films))
        return popular_films
    except Exception as e:
        # Логируем ошибку и пробрасываем её дальше
        logger.error("Ошибка при получении популярных фильмов: %s", e)
        raise

@router.get("/films", response_model=List[Film])
async def get_films_by_genre(
    sort: str = Query("-imdb_rating", description="Поле для сортировки (например, '-imdb_rating')"),
    genre: Optional[str] = Query(None, description="UUID жанра для фильтрации"),
    limit: int = Query(10, ge=1, le=100, description="Количество фильмов в результате (от 1 до 100)"),
    offset: int = Query(0, ge=0, description="Смещение для пагинации (неотрицательное число)"),
    film_service: FilmService = Depends(get_film_service)
) -> List[Film]:
    """
    Эндпоинт для получения фильмов с поддержкой сортировки и фильтрации по жанру.

    :param sort: Поле для сортировки (пример: "-imdb_rating" для убывания).
    :param genre: UUID жанра для фильтрации (пример: <comedy-uuid>).
    :param limit: Максимальное количество фильмов в одном запросе (по умолчанию 10).
    :param offset: Смещение для пагинации (по умолчанию 0).
    :param film_service: Сервис для работы с фильмами.
    :return: Список фильмов (модели Film), отсортированных и отфильтрованных по указанным параметрам.
    """
    logger.info("Получение фильмов: sort=%s, genre=%s, limit=%d, offset=%d", sort, genre, limit, offset)
    try:
        return await film_service.get_films_by_genre(sort=sort, genre=genre, limit=limit, offset=offset)
    except Exception as e:
        logger.error("Ошибка при обработке запроса: %s", e)
        raise