from airflow import DAG
from datetime import datetime
from airflow.providers.apache.hive.hooks.hive import HiveCliHook
from airflow.operators.python import PythonOperator


def hive_hook():
    hive_hook_val = HiveCliHook(hive_cli_conn_id="hive_default").run_cli(
        hql="select count(*) from practice_db.books;"
    ),
    return hive_hook_val


with DAG(
        dag_id="3_2_hive_create_table",
        start_date=datetime(2022, 9, 16),
        schedule_interval=None,
        tags=['airflow_practice']
) as dag:
    create_table = PythonOperator(
        task_id="test",
        python_callable=hive_hook
    )

create_table