import pytest

@pytest.mark.asyncio
async def test_get_all_films(async_client, load_bulk_data_to_es):
    """
    Проверяем, что API возвращает список всех фильмов.
    """
    await load_bulk_data_to_es  # Загружаем тестовые фильмы
    response = await async_client.get("/api/v1/films/")

    assert response.status_code == 200, f"Ожидался 200, но получен {response.status_code}"
    assert isinstance(response.json(), list), "Ответ должен быть списком"
    assert len(response.json()) > 0, "Список фильмов не должен быть пустым"
