import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import logging

from settings import settings

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# SQL-запрос для создания таблицы
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS movies (
    id UUID PRIMARY KEY,
    imdb_rating REAL,
    genres JSONB NOT NULL,
    title VARCHAR(255) NOT NULL,
    description TEXT,
    directors JSONB NOT NULL,
    actors JSONB NOT NULL,
    writers JSONB NOT NULL
);
"""


def create_database_and_table():
    """Создаёт базу данных и таблицу movies."""
    try:
        # Подключаемся к PostgreSQL (системная база данных `postgres`)
        conn = psycopg2.connect(
            dbname="postgres",
            user=settings.postgres_user,
            password=settings.postgres_password,
            host=settings.postgres_host,
            port=settings.postgres_port
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)  # Для создания базы данных
        cursor = conn.cursor()

        # Создаём базу данных, если её нет
        cursor.execute(f"CREATE DATABASE {settings.postgres_name};")
        logger.info(f"База данных {settings.postgres_name} успешно создана.")
    except psycopg2.errors.DuplicateDatabase:
        logger.warning(f"База данных {settings.postgres_name} уже существует.")
    except Exception as e:
        logger.error(f"Ошибка при создании базы данных: {e}")
    finally:
        cursor.close()
        conn.close()

    # Подключаемся к созданной базе данных и создаём таблицу
    try:
        conn = psycopg2.connect(
            dbname=settings.postgres_name,
            user=settings.postgres_user,
            password=settings.postgres_password,
            host=settings.postgres_host,
            port=settings.postgres_port
        )
        cursor = conn.cursor()

        # Выполняем SQL-запрос для создания таблицы
        cursor.execute(CREATE_TABLE_SQL)
        conn.commit()
        logger.info("Таблица movies успешно создана.")
    except Exception as e:
        logger.error(f"Ошибка при создании таблицы: {e}")
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    create_database_and_table()