#!/bin/bash

set -e

# Проверяем, выполнена ли инициализация
if [ ! -f /app/.init_done ]; then
    echo "Запуск предварительных скриптов..."

    python3 /app/src/db/fill_movies.py || { echo "Ошибка при выполнении fill_movies.py"; exit 1; }
    sleep 3
    python3 /app/src/etl/main_etl_genres.py || { echo "Ошибка при выполнении main_etl_genres.py"; exit 1; }
    python3 /app/src/etl/main_etl_persons.py || { echo "Ошибка при выполнении main_etl_persons.py"; exit 1; }
    touch /app/.init_done

    echo "Предварительные скрипты успешно выполнены."
fi

echo "Запуск приложения..."
exec uvicorn src.main:app --host 0.0.0.0 --port 8000