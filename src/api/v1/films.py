import logging
from http import HTTPStatus
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from services.film import FilmService, get_film_service

# Настраиваем логгер
logger = logging.getLogger(__name__)

# Создаём маршрут для работы с фильмами
router = APIRouter()


class Film(BaseModel):
    """
    Модель данных для представления информации о фильме в API.
    """
    id: str
    title: str


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