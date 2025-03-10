import pytest

@pytest.mark.asyncio
@pytest.mark.parametrize(
    "invalid_uuid",
    ["abc", "123456789", "ffffffff-ffff-ffff", "123e4567-e89b-12d3-a456-4266141740000"]  # Убрали ""
)
async def test_get_film_invalid_uuid(async_client, invalid_uuid):
    """
    Проверяем, что API возвращает 422, если передан некорректный UUID.
    """
    url = f"/api/v1/films/{invalid_uuid}"
    print(f"Запрос: {url}")  # Лог запроса
    response = await async_client.get(url)

    print(f"Статус-код ответа: {response.status_code}")  # Лог ответа
    print(f"Тело ответа: {await response.aread()}")  # Лог JSON-ответа (исправленный вызов)

    assert response.status_code == 422, f"Ожидался 422, но получен {response.status_code}"
