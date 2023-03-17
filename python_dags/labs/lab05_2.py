import os
from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime


def get_val_hook():
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    psql_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    list_num = psql_hook.get_records(f"select max(expense) from person_operation")
    os.system(f"echo {list_num[0][0]} >> /tmp/kartashov.txt")


POSTGRES_CONN_ID = "postgres_default"


dag = DAG(
    start_date=datetime(2023, 1, 13),
    dag_id="lab05_2",
    schedule='@daily',
    tags=['lab05']
)

psql_hook = PythonOperator(
    task_id='psql_hook',
    python_callable=get_val_hook
)

psql_hook

