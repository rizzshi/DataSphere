from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def sample_task():
    print("Running sample agent pipeline task.")

def get_dag():
    with DAG(
        dag_id="agent_sample_pipeline",
        start_date=datetime(2023, 1, 1),
        schedule_interval=None,
        catchup=False,
    ) as dag:
        run_task = PythonOperator(
            task_id="run_sample_task",
            python_callable=sample_task
        )
    return dag

dag = get_dag()
