import logging

from tenacity import (retry, retry_if_exception_type, stop_after_attempt,
                      wait_exponential)

logger = logging.getLogger(__name__)  # Логгер для текущего модуля


def with_retry():
    """
    Декоратор для повторных попыток выполнения функции с экспоненциальным ожиданием.

    :return: Декоратор с логированием для повторных попыток.
    """
    return retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(Exception),
        reraise=True,
    )
