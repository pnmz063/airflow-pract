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
    push_nums_list = [randint(0, 9999), randint(0, 9999)]
    ti.xcom_push(key="push_nums", value=push_nums_list)


def pull_nums(ti):
    pull_nums_list = ti.xcom_pull(key="push_nums", task_ids=["test_task_1", "test_task_2", "test_task_3"])
    print(pull_nums_list)


def list_nums(**context):
    my_list_nums = context['ti'].xcom_pull(task_ids=["test_task_1", "test_task_2", "test_task_3"], key="push_nums")[0][0]
    print(my_list_nums)


with DAG(
        dag_id="2_5_xcom_example",
        start_date=datetime(2023, 1, 13),
        description="xcom_example",
        schedule_interval=None,
        tags=["airflow_practice"]
) as dag:
    test_task = [
        PythonOperator(
            task_id=f'test_task_{task}',
            python_callable=push_nums
        ) for task in ["1", "2", "3"]
    ]

    pull_nums = PythonOperator(
        task_id="pull_nums",
        python_callable=pull_nums
    )

    sleep_task = BashOperator(
        task_id="sleep",
        bash_command="sleep 2",
        do_xcom_push=False
    )

    list_nums_task = PythonOperator(
        task_id="list_nums_task",
        python_callable=list_nums,
        provide_context=True
    )

    sleep_task >> test_task >> pull_nums >> list_nums_task
