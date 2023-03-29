from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

POSTGRES_CONN_ID = "postgres_default"
table = "table"

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=1)
}

with DAG(
        dag_id="create_table",
        start_date=datetime(2023, 1, 15),
        description="create_table",
        default_args=default_args,
        schedule_interval=None,
        tags=["lect2"],
        max_active_runs=1,
) as dag:
    create_person = PostgresOperator(
        task_id="create_person",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="""
    create table if not exists person_kartashov_ap (id int4, p_name varchar(30), time_key varchar(10))
    """
    )

    # params тут
    create_person_agg_info = PostgresOperator(
        task_id="create_person_agg_info",
        postgres_conn_id=POSTGRES_CONN_ID,
        params={"table_name": "person_agg_info_kartashov_ap"},
        sql="""
    create table if not exists {{ params.table_name }} (id int4, p_name varchar(30), bank_account int4, time_key varchar(10));
    """
    )

    create_person_op = PostgresOperator(
        task_id="create_person_op",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="""
    create table if not exists person_operation_kartashov_ap (id int4, income int4, expense int4, time_key varchar(10));
    """
    )

create_person >> create_person_agg_info >> create_person_op
