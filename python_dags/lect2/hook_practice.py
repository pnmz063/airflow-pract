from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


def print_val():
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    psql_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    list_num = psql_hook.get_records("select kartashov_num from test_table")
    print(list_num[0][0])


POSTGRES_CONN_ID = "postgres_default"
