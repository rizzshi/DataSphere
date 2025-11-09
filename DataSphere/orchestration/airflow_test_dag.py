from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def sample_task():
    print("Airflow test: DAG executed successfully.")

def get_dag():
    with DAG(
        dag_id="test_agent_dag",
        start_date=datetime(2023, 1, 1),
        schedule=None,
        catchup=False,
    ) as dag:
        run_task = PythonOperator(
            task_id="run_sample_task",
            python_callable=sample_task
        )
    return dag

dag = get_dag()
