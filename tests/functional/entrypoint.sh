#!/bin/sh

# Ожидание готовности Redis
python3 /app/tests/functional/utils/wait_for_redis.py || exit 1

# Ожидание готовности Elasticsearch
python3 /app/tests/functional/utils/wait_for_es.py || exit 1

# Ожидание других сервисов
python3 /app/tests/functional/utils/wait_for_service.py || exit 1

# Запуск тестов
pytest /app/tests/functional/src