from decimal import Decimal
from typing import List
from pydantic import BaseModel, Field, UUID4


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
    uuid: UUID4 = Field(..., description="Уникальный идентификатор жанра")
    name: str = Field(..., min_length=1, max_length=50, description="Название жанра")
    description: str | None = Field(None, description="Описание жанра")


class Person(BaseModel):
    uuid: str
    full_name: str
    films: List[Film] = Field(default_factory=list)