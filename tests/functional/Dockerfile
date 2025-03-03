# Используем официальный образ Python как базовый
FROM python:3.10

# Устанавливаем рабочую директорию
WORKDIR /app

# Копируем requirements.txt и устанавливаем зависимости
COPY ./requirements.txt /app/
RUN pip install -r /app/requirements.txt

# Копируем исходный код приложения и тесты
COPY ../src /app/src
COPY . /app/tests/functional

# Устанавливаем дополнительные зависимости для тестирования
RUN pip install pytest

# Копируем скрипты ожидания готовности сервисов
COPY ./utils/wait_for_es.py /app/tests/functional/utils/wait_for_es.py
COPY ./utils/wait_for_redis.py /app/tests/functional/utils/wait_for_redis.py

# Устанавливаем entrypoint
ENTRYPOINT ["sh", "-c", "python3 /app/tests/functional/utils/wait_for_redis.py && python3 /app/tests/functional/utils/wait_for_es.py && pytest /app/tests/functional/src"]