from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from pprint import pprint


def print_hello_world():
    print("Hello, world! ps: with python")


def print_conext(**context):
    pprint(context)


default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
    "1_1_first_dag",
    start_date=datetime(2023, 1, 15),
    default_args=default_args,
    description="Привет, мир",
    max_active_runs=1,
    schedule_interval=None,
    tags=["airflow_practice", "lect1"]
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

print_context = PythonOperator(
    task_id="print_context",
    python_callable=print_conext,
    dag=dag
)

with_python >> with_bash >> print_context
