from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

dag_name = "create_table"
default_args = {
    'start_date': datetime(2023, 1, 15),
    'retry_delay': timedelta(minutes=5),
    'retries': 2
}

POSTGRES_CONN_ID = "postgres_default"

dag = DAG(
    dag_name,
    default_args=default_args,
    description="create_table",
    tags=["airflow_practice"]
)

create_person = PostgresOperator(
    task_id="create_person",
    postgres_conn_id=POSTGRES_CONN_ID,
    sql="""
    create table if not exists person (id int4, p_name varchar(30), time_key varchar(10))
    """,
    dag=dag
)

create_person_agg_info = PostgresOperator(
    task_id="create_person_agg_info",
    postgres_conn_id=POSTGRES_CONN_ID,
    sql="""
    create table if not exists person_agg_info (id int4, p_name varchar(30), bank_account int8, time_key varchar(10));
    """,
    dag=dag
)

create_person_op = PostgresOperator(
    task_id="create_person_op",
    postgres_conn_id=POSTGRES_CONN_ID,
    sql="""
    create table if not exists person_operation (id int4, income int4, expense int4, time_key varchar(10));
    """,
    dag=dag
)

create_person >> create_person_agg_info >> create_person_op
