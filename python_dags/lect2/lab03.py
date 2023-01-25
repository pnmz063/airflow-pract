from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


def count_lines_from_table(table_name):
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    psql_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    cnt_lines = psql_hook.get_records(f"select count(*) from {table_name}")
    print(cnt_lines)


POSTGRES_CONN_ID = "postgres_default"

create_table_sql = """
        create table if not exists lab03 (
            fio varchar(100),
            dag_name varchar(100),
            exec_date varchar(100),
            prev_exec_date varchar(100),
            prev_exec_date_success varchar(100),
            next_exec_date varchar(100),
            run_id varchar(100)
)
"""

insert_values_sql = """

        insert into lab03 values (
            'kartashov ap',
            '{{ dag.dag_id }}',
            '{{ ds }}',
            '{{ prev_ds }}',
            '{{ prev_start_date_success }}',
            '{{ next_ds }}',
            '{{ run_id }}'
)
"""

with DAG(
        dag_id="lab03",
        start_date=days_ago(2),
        description="lab03",
        schedule_interval=None,
        tags=["airflow_practice", "lab03"]
) as dag:
    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=create_table_sql
    )

    insert_values = PostgresOperator(
        task_id="insert_values",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=insert_values_sql
    )

    count_lines = PythonOperator(
        task_id="count_lines",
        python_callable=count_lines_from_table,
        op_args=['lab03']
    )

    create_table >> insert_values >> count_lines

