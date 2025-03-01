import logging
from http import HTTPStatus
from typing import Dict

from elasticsearch import AsyncElasticsearch
from fastapi import APIRouter, Depends, HTTPException

from src.db.elastic import get_elastic
from src.etl.etl_persons import ETLPersonService

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
handler.setFormatter(formatter)
logger.addHandler(handler)

router = APIRouter()


@router.post("/persons", status_code=HTTPStatus.OK)
async def run_person_etl(
    films_index: str = "films",
    person_index: str = "persons",
    elastic: AsyncElasticsearch = Depends(get_elastic),
) -> Dict[str, str]:
    """
    Запускает ETL процесс для извлечения персон из фильмов и создания индекса
    персон.

    :param films_index: Имя индекса фильмов в Elasticsearch
    (по умолчанию 'films').
    :param person_index: Имя индекса персон в Elasticsearch
    (по умолчанию 'persons').
    :param elastic: Экземпляр клиента Elasticsearch.
    :return: Словарь с сообщением о результате выполнения ETL процесса.
    :raises HTTPException: Возникает при ошибке выполнения ETL процесса.
    """
    logger.info(
        "Получен запрос на запуск ETL процесса для персон: "
        "films_index='%s', person_index='%s'",
        films_index, person_index
    )

    # Инициализируем сервис для выполнения ETL
    etl_service = ETLPersonService(elastic)

    try:
        # Запуск ETL процесса
        logger.info(
            "Запуск ETL процесса для индексов: "
            "films_index='%s', person_index='%s'",
            films_index, person_index
        )
        await etl_service.run_etl(
            films_index=films_index, person_index=person_index
        )
        logger.info("ETL процесс для персон успешно завершён.")
        return {"message": "ETL процесс для персон успешно завершён."}
    except Exception as e:
        # Логируем ошибку и возвращаем HTTPException
        logger.error(
            "Ошибка при выполнении ETL процесса для персон: %s", str(e)
        )
        raise HTTPException(
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            detail=f"Ошибка при выполнении ETL процесса для персон: {str(e)}",
        )
