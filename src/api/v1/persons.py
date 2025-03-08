import logging
from http import HTTPStatus
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from src.models.models import FilmBase, Person
from src.services.person_service import PersonService, get_person_service

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/search", response_model=list[FilmBase])
async def search_persons(
    query: str | None = Query(
        None, description="Поисковый запрос по персонам"
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
    person_service: PersonService = Depends(get_person_service),
) -> list[BaseModel | None]:
    """
    Эндпоинт для поиска фильмов с поддержкой поиска по названию
    и пагинацией.
    """
    persons = await person_service.search_persons(
        query=query, page_size=page_size, page_number=page_number
    )
    if not persons:
        # Выбрасываем HTTP-исключение с кодом 404
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail="Persons not found",
        )

    return persons


@router.get("/{person_id}/film", response_model=FilmBase)
async def person_films(
    person_id: UUID,
    person_service: PersonService = Depends(get_person_service),
) -> list[BaseModel | None]:
    """
    Эндпоинт для получения фильмов в производстве которых участвовала персона.
    """
    films = await person_service.get_person_films(person_id)

    if not films:
        logger.info("Фильмы для персоны с ID: %s не найдены.", person_id)
        # Выбрасываем HTTP-исключение с кодом 404
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail="Films not found",
        )

    return films


@router.get("/{person_id}", response_model=Person)
async def person_details(
    person_id: UUID,
    person_service: PersonService = Depends(get_person_service),
) -> BaseModel:
    """Эндпоинт для получения полной информации о персоне по ID."""
    person_id = str(person_id)
    person = await person_service.get_person_by_id(person_id)

    if not person:
        logger.info("Персона с ID: %s не найдена.", person_id)
        # Выбрасываем HTTP-исключение с кодом 404
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail="Person not found",
        )

    return person
