from decimal import Decimal
from typing import List, Optional
from pydantic import BaseModel


class Film(BaseModel):
    """
    Модель для представления краткой информации о фильме.
    """
    id: str
    title: str
    imdb_rating: Decimal


class FilmFullDescription(BaseModel):
    """
    Модель для представления полной информации о фильме.
    """
    id: str
    title: str
    description: str
    genres: List[str]
    actors: List[str]
    writers: List[str]
    directors: List[str]
    imdb_rating: Decimal
    release_year: int

class Genre(BaseModel):
    """
    Модель для представления жанра.
    """
    id: str  # Уникальный идентификатор жанра
    name: str  # Имя жанра


class FilmRole(BaseModel):
    """
    Модель для представления участия в фильме.
    """
    id: str  # Уникальный идентификатор фильма
    roles: List[str]  # Роли в фильме (например, "actor", "director")


class Person(BaseModel):
    """
    Модель для представления информации о персоне.
    """
    id: str  # Уникальный идентификатор персоны
    full_name: str  # Полное имя персоны
    films: Optional[List[FilmRole]] = []  # Список фильмов, в которых участвовала персона