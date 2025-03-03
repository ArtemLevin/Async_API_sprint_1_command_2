from fastapi import APIRouter, Depends, HTTPException, Query

from src.db.elastic import get_elastic
from src.db.redis_client import get_redis
from src.models.models import Film
from src.services.film_service import FilmService

router = APIRouter()


async def get_film_service(
        elastic=Depends(get_elastic), redis=Depends(get_redis)
) -> FilmService:
    return FilmService(elastic, redis, index_name="films")


@router.get("/", response_model=list[Film])
async def search_films(
        search: str = Query(..., description="Поисковый запрос (название фильма)"),
        page_size: int = Query(10, description="Количество результатов на странице"),
        page_number: int = Query(1, description="Номер страницы"),
        film_service: FilmService = Depends(get_film_service)
):
    """
    Поиск фильмов по названию.

    :param search:
    :param film_service:
    :param query: Строка для поиска (название фильма).
    :param page_size: Количество результатов на странице.
    :param page_number: Номер страницы.
    :return: Список фильмов, соответствующих запросу.
    """
    try:
        # Выполняем поиск фильмов по названию
        films = await film_service.search_films_by_title(search, page_size, page_number)

        # Если ничего не найдено, возвращаем 404
        if not films:
            raise HTTPException(status_code=404, detail="Фильмы не найдены")

        return films

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка при поиске фильмов: {str(e)}")
