FROM python:3.10

WORKDIR /app

# Копирование зависимостей
COPY ./requirements.txt /app/
RUN pip install -r /app/requirements.txt pytest

# Копирование исходного кода и тестов
COPY ../src /app/src
COPY . /app/tests/functional

# Копирование скриптов ожидания
COPY ./utils/wait_for_es.py /app/tests/functional/utils/wait_for_es.py
COPY ./utils/wait_for_redis.py /app/tests/functional/utils/wait_for_redis.py

# Копирование entrypoint
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

# Точка входа
ENTRYPOINT ["/app/entrypoint.sh"]