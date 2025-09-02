from datetime import datetime
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator


def aggregate_daily_metrics():
    conn=psycopg2.connect(
        host="postgres",
        port=5432,
        user="admin",
        password="admin",
        dbname="frauddb",
    )

    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(
        """
            INSERT INTO puplic.predictions_daily_metrics (day, num_predictions, fraud_rate, avg_amount, avg_proba)
            SELECT
                DATE(event_time) AS day,
                COUNT(*) AS num_predictions,
                AVG(prediction)::float AS fraud_rate,
                AVG(amount)::float AS avg_amount,
                AVG(proba)::float AS avg_proba
            FROM public.predictions
            WHERE DATE(event_time) = CURRENT_DATE
            GROUP BY 1
            ON CONFLICT (day) DO UPDATE SET
                num_predictions = EXCLUDED.num_predictions,
                fraud_rate = EXCLUDED.fraud_rate,
                avg_amount = EXCLUDED.avg_amount,
                avg_proba = EXCLUDED.avg_proba;
        """
    )
    cur.close()
    conn.close()



with DAG(
    dag_id='compute_daily_metrics',
    schedule_interval="0 * * * *", # hourly refresh for today's daily row
    start_date=datetime(2025,9,1),
    catchup=False,
    description="Aggregate daily metrics from predictions",
) as dag:
    aggregate=PythonOperator(
        task_id="aggregate_daily",
        python_callable=aggregate_daily_metrics,
    )

    aggregate
