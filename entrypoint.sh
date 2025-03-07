#!/bin/bash

set -e

echo "Запуск предварительных скриптов..."

python3 /app/src/db/fill_movies.py || { echo "Ошибка при выполнении fill_movies.py"; exit 1; }

python3 /app/src/etl/main_etl_genres.py || { echo "Ошибка при выполнении main_etl_genres.py"; exit 1; }

python3 /app/src/etl/main_etl_persons.py || { echo "Ошибка при выполнении main_etl_persons.py"; exit 1; }

echo "Предварительные скрипты успешно выполнены."

echo "Запуск приложения..."
exec uvicorn src.main:app --host 0.0.0.0 --port 8000