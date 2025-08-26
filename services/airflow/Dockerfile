FROM apache/airflow:2.9.2-python3.10

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
  && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir \
    mlflow==2.14.1 \
    scikit-learn==1.4.2 \
    pandas==2.2.2 \
    numpy==1.26.4 \
    psycopg2-binary==2.9.9





