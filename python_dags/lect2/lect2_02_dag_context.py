from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from pprint import pprint


def print_hello_world():
    print("Hello, world! my_exec_date = {{ ds }}")


def print_hello_world2(**context):
    print(f"Hello, world! my_exec_date {context['ds']}")


def pprint_context(**context):
    pprint(context)


default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
    "dag_context_practice",
    start_date=datetime(2023, 2, 6),
    default_args=default_args,
    description="Привет, мир",
    max_active_runs=1,
    schedule_interval="0 9 * * *",
    tags=["lect2"]
)

bash = BashOperator(
    task_id="bash_task",
    bash_command="echo Hello, world! my_exec_date = {{ ds }}",
    dag=dag
)

python_without_context = PythonOperator(
    task_id="python_without_context",
    python_callable=print_hello_world,
    dag=dag
)

python_with_context = PythonOperator(
    task_id="python_with_context",
    python_callable=print_hello_world2,
    provide_context=True,
    dag=dag
)


print_context = PythonOperator(
    task_id="print_context",
    provide_context=True,
    python_callable=pprint_context,
    dag=dag
)

bash >> python_without_context >> python_with_context >> print_context
