#!/usr/bin/env bash

# Activate the virtual environment
source /opt/airflow/venv/bin/activate
echo "Initializing the Airflow database..."
airflow db init

echo "Creating admin user..."
airflow users create \
    --username admin \
    --firstname Ebrahim \
    --lastname Emad \
    --role Admin \
    --email admin@example.com \
    --password admin

echo "Starting $1..."
exec airflow "$1"
