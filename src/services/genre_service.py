import logging
from typing import List, Optional
from uuid import UUID

from src.models.models import Genre
from src.services.base_service import BaseService

logger = logging.getLogger(__name__)


class GenreService(BaseService):
    """
    Сервис для работы с жанрами.
    """

    def get_cache_key(self, unique_id: UUID) -> str:
        """
        Генерация ключа для кеша.

        :param unique_id: Уникальный ID жанра (UUID).
        :return: Строка-ключ для кеша.
        """
        return f"genre:{unique_id}"

    def parse_elastic_response(self, response: dict) -> Genre | None:
        """
        Преобразует данные Elasticsearch в словарь для API-ответа.

        :param response: Данные жанра из Elasticsearch (словарь).
        :return: Словарь с данными жанра.
        """
        try:
            # Извлекаем данные из "_source"
            source = response["_source"]
            # Преобразуем данные в объект модели

            return Genre(
                id=UUID(source["id"]),
                name=source["name"]
            )
        except KeyError as e:
            logger.error(f"Ошибка парсинга данных Elasticsearch: отсутствует ключ {e}")
            raise ValueError(f"Неверный формат данных: отсутствует ключ {e}")
        except Exception as e:
            logger.error(f"Ошибка при преобразовании данных в модель: {e}")
            raise

    async def get_all_genres(self) -> List[Genre]:
        """
        Получает все жанры из Elasticsearch и возвращает их в формате API-ответа.

        :return: Список словарей с данными жанров.
        """
        try:
            # Получаем сырые данные из Elasticsearch
            raw_genres = await self.elastic_service.search(index="genres", body={"query": {"match_all": {}}})

            # Фильтруем только успешные результаты
            genres = [
                self.parse_elastic_response(hit)
                for hit in raw_genres.get("hits", {}).get("hits", [])
                if self.parse_elastic_response(hit) is not None
            ]

            # Преобразуем объекты Genre в словари для API-ответа
            return genres

        except Exception as e:
            logger.error(f"Ошибка при получении жанров: {e}")
            raise

    async def get_genre_by_id(self, genre_id: UUID) -> Optional[dict]:
        """
        Получение данных о жанре по его уникальному ID.

        :param genre_id: Уникальный ID жанра (UUID).
        :return: Словарь с данными жанра или None, если жанр не найден.
        """
        try:
            # Формируем запрос для поиска по ID
            query = {
                "query": {
                    "term": {
                        "id": str(genre_id)  # Преобразуем UUID в строку для запроса
                    }
                }
            }

            # Выполняем запрос к Elasticsearch
            response = await self.elastic_service.search(index="genres", body=query)

            # Проверяем, есть ли результаты
            hits = response.get("hits", {}).get("hits", [])
            if not hits:
                logger.warning(f"Жанр с ID {genre_id} не найден.")
                return None

            # Парсим первый результат
            genre_data = hits[0]
            return self.parse_elastic_response(genre_data)

        except Exception as e:
            logger.error(f"Ошибка при поиске жанра с ID {genre_id}: {e}")
            raise