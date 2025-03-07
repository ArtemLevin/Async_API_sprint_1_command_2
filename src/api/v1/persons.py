from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query

from src.db.elastic import get_elastic
from src.db.redis_client import get_redis
from src.models.models import Person
from src.services.person_service import PersonService

router = APIRouter()


async def get_person_service(
    elastic=Depends(get_elastic), redis=Depends(get_redis)
) -> PersonService:
    return PersonService(elastic, redis, index_name="persons")

@router.get("/persons_by_uuid/{person_uuid}", response_model=Person)
async def get_person(
    person_uuid: UUID, person_service: PersonService = Depends(get_person_service)
):
    """Эндпоинт для получения информации о персоне по UUID."""
    person = await person_service.get_by_uuid(person_uuid)
    if not person:
        raise HTTPException(status_code=404, detail="Персона не найдена")
    return person

@router.get("/", response_model=list[Person])
async def get_all_persons(
    person_service: PersonService = Depends(get_person_service)
):
    """
    Получение списка всех персон.
    """
    persons = await person_service.get_all_persons()
    return persons

@router.get("/persons_by_full_name/", response_model=list[Person])
async def search_persons_by_full_name(
        search: str = Query(..., description="Поисковый запрос (название фильма)"),
        page_size: int = Query(10, description="Количество результатов на странице"),
        page_number: int = Query(1, description="Номер страницы"),
        person_service: PersonService = Depends(get_person_service)
):
    """
    Поиск фильмов по названию.

    :param search:
    :param person_service:
    :param query: Строка для поиска (название фильма).
    :param page_size: Количество результатов на странице.
    :param page_number: Номер страницы.
    :return: Список фильмов, соответствующих запросу.
    """
    try:
        # Выполняем поиск фильмов по названию
        films = await person_service.search_persons_by_full_name(search, page_size, page_number)

        # Если ничего не найдено, возвращаем 404
        if not films:
            raise HTTPException(status_code=404, detail="Фильмы не найдены")

        return films

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка при поиске фильмов: {str(e)}")