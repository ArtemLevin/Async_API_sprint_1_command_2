from decimal import Decimal
from typing import List
from uuid import UUID

from pydantic import BaseModel, Field, UUID4


class Genre(BaseModel):
    uuid: UUID4 = Field(..., description="Уникальный идентификатор жанра")
    name: str = Field(..., min_length=1, max_length=50, description="Название жанра")
    description: str | None = Field(None, description="Описание жанра")


class Person(BaseModel):
    uuid: str
    full_name: str
    films: List[Film] = Field(default_factory=list)


class GenreBase(BaseModel):
    """
    Модель для представления жанра.
    """
    id: UUID = Field(serialization_alias='uuid')  # Уникальный идентификатор жанра
    name: str  # Имя жанра


class PersonBase(BaseModel):
    """
    Базовая модель для представления информации о персоне.
    """
    id: UUID = Field(serialization_alias='uuid')  # Уникальный идентификатор персоны
    full_name: str  # Полное имя персоны


class Film(BaseModel):
    """
    Модель для представления информации о фильме.
    """
    id: UUID = Field(serialization_alias="uuid")
    title: str
    description: str
    genre: list[GenreBase]
    actors: list[PersonBase]
    writers: list[PersonBase]
    directors: list[PersonBase]
    imdb_rating: Decimal
