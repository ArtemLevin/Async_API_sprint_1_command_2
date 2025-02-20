from fastapi import APIRouter, HTTPException
from uuid import UUID

from src.db.elastic import get_elastic
from src.db.redis import get_redis
from src.services.person_service import PersonService
from src.models.models import Person


router = APIRouter()

person_service = PersonService(get_elastic, get_redis, index_name="persons")

@router.get("/{person_uuid}", response_model=Person)
async def get_person(person_uuid: UUID):
    """Эндпоинт для получения информации о персоне по UUID."""

    person = await person_service.get_by_uuid(person_uuid)
    if not person:
        raise HTTPException(status_code=404, detail="Персона не найдена")
    return person

@router.get("/", response_model=list[Person])
async def get_all_persons():
    """
    Получение списка всех персон.
    """
    persons = await person_service.get_all_persons()
    return persons