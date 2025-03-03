FROM python:3.10

WORKDIR /app

COPY ./requirements.txt /app/
RUN pip install -r /app/requirements.txt

COPY ../src /app/src
COPY . /app/tests/functional

RUN pip install pytest

COPY ./utils/wait_for_es.py /app/tests/functional/utils/wait_for_es.py
COPY ./utils/wait_for_redis.py /app/tests/functional/utils/wait_for_redis.py

ENTRYPOINT ["sh", "-c", "python3 /app/tests/functional/utils/wait_for_redis.py && python3 /app/tests/functional/utils/wait_for_es.py && pytest /app/tests/functional/src"]