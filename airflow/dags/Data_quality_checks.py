from datetime import datetime, timedelta
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(
    dag_id='data_quality_checks',
    schedule_interval="*/10 * * * *",  # every 10 minutes
    start_date=datetime(2025, 8, 28),
    catchup=False,
    description="Basic data quality checks on predictions table",

) as dag:
    run_checks=PythonOperator(
        task_id="run_checks",
        python_callable=dq_checks,
    )
    run_checks