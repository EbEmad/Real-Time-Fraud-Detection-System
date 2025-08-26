FROM python:3.10-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN pip install --no-cache-dir kafka-python==2.0.2 numpy==1.26.4 faker==24.9.0 python-dateutil==2.9.0.post0

COPY ./app /app/

ENV KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
    KAFKA_TOPIC=transactions \
    PRODUCE_RATE_PER_SEC=2

CMD python -u app/producer.py



