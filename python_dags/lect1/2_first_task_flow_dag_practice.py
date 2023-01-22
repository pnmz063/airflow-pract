from airflow.decorators import dag, task
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=1)
}

@dag(
    dag_id="1_2_first_task_flow_api_dag",
    start_date=datetime(2023, 1, 15),
    default_args=default_args,
    description="Привет, мир и task flow api",
    schedule_interval=None,
    tags=["airflow_practice", "lect1"]
)
def first_task_flow_api_dag():
    @task
    def print_hello_world():
        print("Hello, world!")

    @task
    def print_something_else(prev_task):
        print("and something else")

    print_something_else(print_hello_world())


first_task_flow_api_dag()
