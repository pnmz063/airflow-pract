# bash_op как xcom
# использовать template для xcom
# динамическое создание дагов
# рендер xcom, provide_context

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from random import randint


def push_nums(ti):
    push_val = "this is xcom"
    ti.xcom_push(key="push_nums", value=push_val)


def pull_nums(ti):
    pull_str = ti.xcom_pull(key="push_nums", task_ids=["test_push"])
    print(pull_str)


with DAG(
        dag_id="xcom_example2",
        start_date=datetime(2023, 1, 13),
        description="xcom_example2",
        schedule_interval=None,
        tags=["lect3"]
) as dag:
    test_push = PythonOperator(
        task_id='test_push',
        python_callable=push_nums,
    )

    pull_nums = PythonOperator(
        task_id="pull_nums",
        python_callable=pull_nums
    )



    sleep_task = BashOperator(
        task_id="sleep",
        bash_command="sleep 2",
        do_xcom_push=False
    )

    sleep_task >> test_push >> pull_nums
