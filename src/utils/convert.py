from decimal import Decimal


def convert_decimals(obj):
    """
    Рекурсивно преобразует все объекты decimal.Decimal в float.
    """
    if isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, dict):
        return {k: convert_decimals(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_decimals(i) for i in obj]
    return obj