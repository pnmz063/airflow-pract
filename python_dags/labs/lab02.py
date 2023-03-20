from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

POSTGRES_CONN_ID = "postgres_default"

with DAG(
        "lab02",
        start_date=days_ago(2),
        schedule_interval=None,
        tags=["lab02"]
) as dag:
    table_for_test_hook = PostgresOperator(
        task_id="test_table",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=""" 
        insert into lab02 (name, num) values ('kartashovap', 555) on conflict (name) do update set num = 555
            """
    )

    table_for_test_hook
