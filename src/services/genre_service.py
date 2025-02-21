import logging
from uuid import UUID
from typing import Optional, List, Dict, Any

from services.base_service import BaseService
from models.models import Genre

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)  # Устанавливаем уровень логирования


class GenreService(BaseService):
    """
    Сервис для работы с жанрами.
    """

    def get_cache_key(self, unique_id: UUID) -> str:
        """
        Генерация ключа для кэша на основе UUID жанра.

        Args:
            unique_id (UUID): Уникальный идентификатор жанра.

        Returns:
            str: Ключ в формате 'genre:{UUID}'.

        Raises:
            ValueError: Если передан пустой UUID.
        """
        if not unique_id:
            logger.error("UUID не может быть пустым при генерации ключа кэша.")
            raise ValueError("UUID не может быть пустым.")
        return f"genre:{unique_id}"

    def parse_elastic_response(self, response: Dict[str, Any]) -> Optional[Genre]:
        """
        Парсинг данных из ответа Elasticsearch в объект модели `Genre`.

        Args:
            response (Dict[str, Any]): Ответ Elasticsearch, содержащий данные о жанре.

        Returns:
            Optional[Genre]: Объект `Genre` или None, если парсинг не удался.
        """
        try:
            source = response["_source"]
            return Genre(
                uuid=source["uuid"],
                name=source["name"],
                description=source.get("description")  # Описание может отсутствовать
            )
        except KeyError as e:
            logger.error(f"Ошибка парсинга данных Elasticsearch: отсутствует ключ {e}. Ответ: {response}")
            return None
        except Exception as e:
            logger.error(f"Неизвестная ошибка при парсинге данных Elasticsearch: {e}. Ответ: {response}")
            return None

    async def get_all_genres(self) -> List[Genre]:
        """
        Получение списка всех жанров из Elasticsearch.

        Returns:
            List[Genre]: Список объектов `Genre`.
        """
        try:
            genres = await self.get_all()  # Используем метод из базового сервиса
            logger.info(f"Получено {len(genres)} жанров из Elasticsearch.")
            return genres
        except Exception as e:
            logger.error(f"Ошибка при получении всех жанров: {e}")
            return []

    async def get_genre_by_uuid(self, unique_id: UUID) -> Optional[Genre]:
        """
        Получение информации о жанре по его UUID.

        Args:
            unique_id (UUID): Уникальный идентификатор жанра.

        Returns:
            Optional[Genre]: Объект `Genre` или None, если жанр не найден.
        """
        try:
            genre = await self.get_by_uuid(unique_id)
            if genre:
                logger.info(f"Жанр с UUID '{unique_id}' успешно найден.")
            else:
                logger.warning(f"Жанр с UUID '{unique_id}' не найден.")
            return genre
        except Exception as e:
            logger.error(f"Ошибка при получении жанра с UUID '{unique_id}': {e}")
            return None