FROM python:3.10

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Убедимся, что .env добавлен в контейнер
COPY .env .env

CMD ["python", "main.py"]