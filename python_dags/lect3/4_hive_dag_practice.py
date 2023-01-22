from airflow import DAG
from datetime import datetime
from airflow.providers.apache.hive.hooks.hive import HiveCliHook
from airflow.operators.python import PythonOperator
from datetime import timedelta


def hive_hook():
    hive_hook_val = HiveCliHook(hive_cli_conn_id="hive_default").run_cli(
        hql="select count(*) from practice_db.books;"
    ),
    return hive_hook_val


default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=1)
}

with DAG(
        dag_id="3_2_hive_create_table",
        start_date=datetime(2022, 9, 16),
        schedule_interval=None,
        default_args=default_args,
        tags=['airflow_practice']
) as dag:
    create_table = PythonOperator(
        task_id="test",
        python_callable=hive_hook
    )

create_table