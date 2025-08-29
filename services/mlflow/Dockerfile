FROM python:3.10-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
  && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    mlflow==2.14.1 \
    psycopg2-binary==2.9.9 \
    gunicorn==21.2.0

ENV BACKEND_STORE_URI="postgresql+psycopg2://admin:admin@postgres:5432/frauddb" \
    ARTIFACT_ROOT="/mlflow/artifacts"

EXPOSE 5000

VOLUME ["/mlflow/artifacts"]

CMD mlflow server \
    --host 0.0.0.0 \
    --port 5000 \
    --backend-store-uri "$BACKEND_STORE_URI" \
    --artifacts-destination "$ARTIFACT_ROOT"



