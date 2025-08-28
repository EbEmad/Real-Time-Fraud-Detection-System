from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="train_and_register",
    schedule_interval='@daily',
    start_date=datetime(2025, 8, 28),
    catchup=False,
    description="Train fraud model and register in MLflow",
) as dag:
    train=BashOperator(
        task_id="train_model",
        bash_command="python /opt/airflow/train_app/training_main.py",
        env={
            "MLFLOW_TRACKING_URI": "http://mlflow:5000",
            "MLFLOW_EXPERIMENT_NAME": "fraud_detection",
            "REGISTERED_MODEL_NAME": "fraud_detector",
        }
    )
    train