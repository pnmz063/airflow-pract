from airflow.decorators import dag, task
from datetime import datetime


@dag(
    dag_id="1_2_first_task_flow_api_dag",
    start_date=datetime(2023, 1, 15),
    description="Привет, мир и task flow api",
    schedule_interval=None,
    tags=["airflow_practice"]
)
def first_task_flow_api_dag():

    @task
    def print_hello_world():
        print("Hello, world!")

    @task
    def print_something_else():
        print("and something else")

    print_hello_world()
    print_something_else()


first_task_flow_api_dag()