from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime


def print_hello_world():
    print("Hello, world! ps: with python")


dag = DAG(
    "1_1_first_dag",
    start_date=datetime(2023, 1, 15),
    description="Привет, мир",
    schedule_interval=None,
    tags=["airflow_practice"]
)

with_bash = BashOperator(
    task_id="bash_task",
    bash_command="echo Hello, world! ps: with bash",
    dag=dag
)

with_python = PythonOperator(
    task_id="python_task",
    python_callable=print_hello_world,
    dag=dag
)

with_python >> with_bash
