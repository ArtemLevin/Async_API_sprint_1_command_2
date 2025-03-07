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


@router.get("/films_by_title/", response_model=list[Film])
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


@router.get("/films_by_description/", response_model=list[Film])
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
        films = await film_service.search_films_by_description(search, page_size, page_number)

        # Если ничего не найдено, возвращаем 404
        if not films:
            raise HTTPException(status_code=404, detail="Фильмы не найдены")

        return films

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка при поиске фильмов: {str(e)}")

@router.get("/films_by_actor/", response_model=list[Film])
async def search_films_by_actor(
        search: str = Query(..., description="Поисковый запрос (имя актёра)"),
        page_size: int = Query(10, description="Количество результатов на странице"),
        page_number: int = Query(1, description="Номер страницы"),
        film_service: FilmService = Depends(get_film_service)
):
    """
    Поиск фильмов по имени актёра.

    :param search: Строка для поиска (имя актёра).
    :param page_size: Количество результатов на странице.
    :param page_number: Номер страницы.
    :param film_service: Сервис для работы с фильмами.
    :return: Список фильмов, в которых есть актёр, удовлетворяющий запросу.
    """
    try:
        # Выполняем поиск фильмов по имени актёра
        films = await film_service.search_films_by_actor(search, page_size, page_number)

        # Если фильмы не найдены, возвращаем 404
        if not films:
            raise HTTPException(status_code=404, detail="Фильмы не найдены")

        return films

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка при поиске фильмов: {str(e)}")

@router.get("/films_by_writer/", response_model=list[Film])
async def search_films_by_writer(
        search: str = Query(..., description="Поисковый запрос (имя сценариста)"),
        page_size: int = Query(10, description="Количество результатов на странице"),
        page_number: int = Query(1, description="Номер страницы"),
        film_service: FilmService = Depends(get_film_service)
):
    """
    Поиск фильмов по имени сценариста (writer).

    :param search: Строка для поиска (имя сценариста).
    :param page_size: Количество результатов на странице.
    :param page_number: Номер страницы.
    :param film_service: Сервис для работы с фильмами.
    :return: Список фильмов, где найден сценарист с указанным именем.
    """
    try:
        films = await film_service.search_films_by_writer(search, page_size, page_number)
        if not films:
            raise HTTPException(status_code=404, detail="Фильмы не найдены")
        return films

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка при поиске фильмов: {str(e)}")

@router.get("/films_by_director/", response_model=list[Film])
async def search_films_by_director(
        search: str = Query(..., description="Поисковый запрос (имя режиссёра)"),
        page_size: int = Query(10, description="Количество результатов на странице"),
        page_number: int = Query(1, description="Номер страницы"),
        film_service: FilmService = Depends(get_film_service)
):
    """
    Поиск фильмов по имени режиссёра (director).

    :param search: Строка для поиска (имя режиссёра).
    :param page_size: Количество результатов на странице.
    :param page_number: Номер страницы.
    :param film_service: Сервис для работы с фильмами.
    :return: Список фильмов, где найден режиссёр с указанным именем.
    """
    try:
        films = await film_service.search_films_by_director(search, page_size, page_number)
        if not films:
            raise HTTPException(status_code=404, detail="Фильмы не найдены")
        return films

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка при поиске фильмов: {str(e)}")

@router.get("/films_by_genre/", response_model=list[Film])
async def search_films_by_genre(
        search: str = Query(..., description="Поисковый запрос (название жанра)"),
        page_size: int = Query(10, description="Количество результатов на странице"),
        page_number: int = Query(1, description="Номер страницы"),
        film_service: FilmService = Depends(get_film_service)
):
    """
    Поиск фильмов по названию жанра.

    :param search: Строка для поиска (название жанра).
    :param page_size: Количество результатов на странице.
    :param page_number: Номер страницы.
    :param film_service: Сервис для работы с фильмами.
    :return: Список фильмов, где найден указанный жанр.
    """
    try:
        films = await film_service.search_films_by_genre(search, page_size, page_number)
        if not films:
            raise HTTPException(status_code=404, detail="Фильмы не найдены")
        return films

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка при поиске фильмов: {str(e)}")