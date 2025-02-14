from pydantic import BaseSettings


class Settings(BaseSettings):
    # Настройки PostgreSQL
    postgres_name: str
    postgres_user: str
    postgres_password: str
    postgres_host: str
    postgres_port: int

    # Настройки Elasticsearch
    elastic_host: str
    movies_index: str

    # Файл состояния
    state_file: str

    # Запрос для выборки данных
    movies_query: str = """
        SELECT id, imdb_rating, genres, title, description, directors, actors, writers
        FROM public.movies
        WHERE imdb_rating > %s
        ORDER BY id ASC;
    """

    class Config:
        # Указываем, что значения будут подгружаться из `.env` файла
        env_file = ".env"
        env_file_encoding = "utf-8"
        # Указываем, что переменные окружения маппятся в snake_case
        alias_generator = lambda s: s.lower()
        allow_population_by_field_name = True


# Инициализация настроек
settings = Settings()