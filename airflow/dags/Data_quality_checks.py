from datetime import datetime, timedelta
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator

def dq_checks():
    conn=psycopg2.connect(
        host="postgres",
        port=5432,
        user="admin",
        password="admin",
        dbname="frauddb",
    )

    conn.autocommit=True
    cur=conn.cursor()

    #  Recent data exists

    cur.execute(
        """
        select count(*)
        from public.predictions
        where event_time > NOW() - interval '15 minutes'
        """
    )
    recent_count=cur.fetchone()[0]
    if recent_count<1:
        raise AssertionError("No predictions in the last 15 minutes")

    #  Null checks
    cur.execute("SELECT COUNT(*) FROM public.predictions WHERE transaction_id IS NULL")
    if cur.fetchone()[0]>0:
        raise AssertionError("NULL transaction_id found")

    #  Fraud rate sanity
    cur.execute("SELECT AVG(prediction)::float FROM public.predictions WHERE event_time > NOW() - interval '1 hour'")
    val = cur.fetchone()[0]
    if val is None or not (0.0<=val<=1.0):
        raise AssertionError("Fraud rate out of bounds [0,1]")
    cur.close()
    conn.close()


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