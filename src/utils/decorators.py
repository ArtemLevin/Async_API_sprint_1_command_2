import logging
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
    RetryCallState
)

logger = logging.getLogger(__name__)  # Логгер для текущего модуля


def log_before_retry(retry_state: RetryCallState):
    """
    Логирование перед повторной попыткой.
    """
    logger.warning(
        "Попытка #%d завершилась с ошибкой. Повтор через %.2f секунд. "
        "Исключение: %s",
        retry_state.attempt_number,
        retry_state.next_action.sleep,
        retry_state.outcome.exception()
    )


def log_retry_error(retry_state: RetryCallState):
    """
    Логирование окончательной ошибки после всех попыток.
    """
    logger.error(
        "Все %d попытки исчерпаны. Последняя ошибка: %s",
        retry_state.attempt_number,
        retry_state.outcome.exception()
    )


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
        before=log_before_retry,  # Логирование перед повторной попыткой
        after=log_retry_error  # Логирование после всех попыток
    )