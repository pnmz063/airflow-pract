from airflow import DAG
from datetime import datetime
from airflow.providers.apache.hive.operators.hive import HiveOperator

from datetime import timedelta


default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=3)
}

with DAG(
        "hive_test",
        start_date=datetime(2023, 1, 31),
        default_args=default_args,
        description="hive_test",
        max_active_runs=1,
        tags=['hive']
) as dag:
    create = HiveOperator(
        task_id='create_hive',
        hive_cli_conn_id="hive_default",
        hql="""
        CREATE EXTERNAL TABLE IF NOT EXISTS practice_db.test_table (id integer, name string)
        """
    )

    insert = HiveOperator(
        task_id='insert_hive',
        hql="""
        insert into practice_db.test_table VALUES (1, 'test')
        """
    )

create >> insert
