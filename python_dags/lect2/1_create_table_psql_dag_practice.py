from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

POSTGRES_CONN_ID = "postgres_default"

with DAG(
        dag_id="2_1_create_table",
        start_date=datetime(2023, 1, 15),
        description="create_table",
        schedule_interval=None,
        tags=["airflow_practice"]
) as dag:
    create_person = PostgresOperator(
        task_id="create_person",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="""
    create table if not exists person_kartashov_ap (id int4, p_name varchar(30), time_key varchar(10))
    """
    )

    create_person_agg_info = PostgresOperator(
        task_id="create_person_agg_info",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="""
    create table if not exists person_agg_info_kartashov_ap (id int4, p_name varchar(30), bank_account int4, time_key varchar(10));
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
