FROM python:3.10

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
ENV PYTHONPATH=/app

WORKDIR /app

COPY requirements.txt /app/requirements.txt

RUN pip install --upgrade pip && \
    pip install -r requirements.txt --no-cache-dir

COPY . /app

CMD python3 /app/src/db/fill_movies.py && \
    python3 /app/src/etl/main_etl_genres.py && \
    python3 /app/src/etl/main_etl_persons.py && \
    uvicorn src.main:app --host 0.0.0.0 --port 8000
#     python3 /app/src/etl/main_etl_persons.py && \
#     python3 /app/src/etl/main_etl_genres.py && \