from decimal import Decimal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class GenreBase(BaseModel):
    """
    Базовая модель для представления жанра.
    """
    id: UUID = Field(
        ...,
        description="Уникальный идентификатор жанра",
        serialization_alias='uuid'
    )
    name: str = Field(
        ..., min_length=1, max_length=50, description="Название жанра"
    )


class Genre(GenreBase):
    """
    Модель для представления жанра.
    """
    description: str | None = Field(None, description="Описание жанра")


class PersonBase(BaseModel):
    """
    Базовая модель для представления информации о персоне.
    """
    id: UUID = Field(
        ...,
        description="Уникальный идентификатор персоны",
        serialization_alias='uuid'
    )
    full_name: str = Field(
        ..., min_length=1, max_length=150, description="Полное имя персоны"
    )


class FilmRole(BaseModel):
    """
    Модель для представления участия в фильме.
    """
    id: str = Field(
        ...,
        description="Уникальный идентификатор фильма",
        serialization_alias="uuid"
    )
    roles: list[str] = Field(
        ..., min_length=1, max_length=100, description="Роли персоны в фильме"
    )


class Person(PersonBase):
    """
    Модель для представления информации о персоне.
    """
    films: list[FilmRole] = Field(
        default_factory=list, description="Роли персоны в фильмах"
    )


class FilmBase(BaseModel):
    """
    Базовая модель для представления кратной информации по фильму.
    """
    id: UUID = Field(
        ...,
        description="Уникальный идентификатор фильма",
        serialization_alias="uuid",
    )
    title: str = Field(
        ..., min_length=1, max_length=255, description="Название фильма"
    )
    imdb_rating: Decimal = Field(
        ..., ge=1, le=10, description="Рейтинг фильма по версии IMDb"
    )

    # Автоматическое преобразование Decimal → float
    model_config = ConfigDict(json_encoders={Decimal: float})


class Film(FilmBase):
    """
    Модель для представления информации о фильме.
    """
    description: str = Field(..., description="Описание фильма")
    genre: list[GenreBase] = Field(
        ..., description="Список жанров фильма"
    )
    actors: list[PersonBase] = Field(..., description="Список актёров фильма")
    writers: list[PersonBase] = Field(
        ..., description="Список сценаристов фильма"
    )
    directors: list[PersonBase] = Field(
        ..., description="Список режиссёров фильма"
    )
