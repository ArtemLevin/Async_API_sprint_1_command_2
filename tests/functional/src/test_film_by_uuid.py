import pytest


@pytest.mark.asyncio
async def test_get_film_by_uuid(async_client, load_bulk_data_to_es):
    """
    Проверяем, что можно получить фильм по UUID.
    """
    test_film = (await load_bulk_data_to_es)[0]  # Загружаем тестовые данные
    film_uuid = test_film["uuid"]

    response = await async_client.get(f"/api/v1/films/{film_uuid}")
    assert response.status_code == 200, f"Ожидался 200, но получен {response.status_code}"

    # Логируем ответ сервера
    data = response.json()
    print("Ответ сервера:", data)  # <-- Вывод JSON для проверки

    # Исправленный тест: Проверяем "uuid" вместо "id"
    assert "uuid" in data, "Ответ не содержит uuid"
    assert data["uuid"] == film_uuid, f"UUID в ответе не совпадает: ожидался {film_uuid}, получен {data['uuid']}"
