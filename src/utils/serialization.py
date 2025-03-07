import json
from decimal import Decimal
from uuid import UUID
from typing import Any


def model_to_dict(model: Any) -> dict:
    """
    Преобразует объект модели в словарь.

    Если объект является экземпляром:
      - Pydantic модели, используется метод dict(),
      - имеет метод to_dict(), он будет вызван,
      - содержит типы, не сериализуемые по умолчанию (Decimal, UUID), то они преобразуются к строке.

    :param model: Объект модели.
    :return: Словарь, готовый для сериализации в JSON.
    """
    # Если объект имеет метод "to_dict", используем его
    if hasattr(model, "to_dict") and callable(getattr(model, "to_dict")):
        data = model.to_dict()
    # Если это экземпляр Pydantic BaseModel, можно использовать метод dict()
    elif hasattr(model, "dict") and callable(getattr(model, "dict")):
        data = model.dict()
    # Если объект не поддерживает ни один из методов, пробуем взять его __dict__
    elif hasattr(model, "__dict__"):
        data = model.__dict__
    else:
        raise TypeError(f"Объект типа {model.__class__.__name__} не может быть преобразован в словарь.")

    # Рекурсивно обрабатываем вложенные объекты
    def serialize_value(value: Any) -> Any:
        if isinstance(value, (str, int, float, bool)) or value is None:
            return value
        if isinstance(value, Decimal):
            return str(value)
        if isinstance(value, UUID):
            return str(value)
        if isinstance(value, list):
            return [serialize_value(item) for item in value]
        if isinstance(value, dict):
            return {key: serialize_value(val) for key, val in value.items()}
        # Если объект имеет метод to_dict
        if hasattr(value, "to_dict") and callable(getattr(value, "to_dict")):
            return serialize_value(value.to_dict())
        # Если объект имеет метод dict (например, pydantic.BaseModel)
        if hasattr(value, "dict") and callable(getattr(value, "dict")):
            return serialize_value(value.dict())
        return str(value)

    return serialize_value(data)


# Опционально можно добавить функцию, которая сразу выдаст JSON-строку:
def model_to_json(model: Any) -> str:
    return json.dumps(model_to_dict(model))