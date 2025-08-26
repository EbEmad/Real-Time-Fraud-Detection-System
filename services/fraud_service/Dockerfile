FROM python:3.10-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
  && rm -rf /var/lib/apt/lists/*

COPY services/fraud_service/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY ./app /app/

EXPOSE 8000

ENV KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
    KAFKA_TOPIC=transactions \
    POSTGRES_URI=postgresql+psycopg2://admin:admin@postgres:5432/frauddb \
    MLFLOW_TRACKING_URI=http://mlflow:5000 \
    MLFLOW_MODEL_NAME=fraud_detector \
    MLFLOW_MODEL_STAGE=Production

CMD uvicorn app.main:app --host 0.0.0.0 --port 8000
