from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


def is_odd(num):
    if num % 2 == 0:
        print(f"{num} is even")
    else:
        print(f"{num} is odd")


def get_val():
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    psql_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    list_num = psql_hook.get_records("select kartashov_num from test_table")
    print(list_num[0][0])
    return list_num[0][0]


POSTGRES_CONN_ID = "postgres_default"

with DAG(
    "lab02",
    start_date=days_ago(2),
    schedule_interval=None,
    tags=["lect2"]
) as dag:
    table_for_test_hook = PostgresOperator(
        task_id="test_table",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="""create table if not exists test_table (kartashov_num int4); 
            insert into test_table values (555);
            """
    )

    psql_hook = PythonOperator(
        task_id="psql_hook",
        python_callable=get_val
    )

    check_num = PythonOperator(
        task_id="check_num",
        python_callable=is_odd,
        op_kwargs={"num": get_val()}
    )

    table_for_test_hook >> psql_hook >> check_num



from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def is_odd(num):
    if num % 2 == 0:
        print(f"{num} is even")
    else:
        print(f"{num} is odd")

def take_num():
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    psql_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    take_num = psql_hook.get_records(f"select sum(id) from test_table_volkova")
    return take_num[0][0]


POSTGRES_CONN_ID = "postgres_default"

default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=1)
}

with DAG(
        dag_id="lab_2_NEW_Volkova",
        start_date=datetime(2023, 1, 15),
        description="create_table",
        default_args=default_args,
        schedule_interval=None,
        tags=["airflow_practice", "lect2"]
) as dag:
    drop = PostgresOperator(
        task_id="drop",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql='DROP TABLE IF EXISTS test_table_volkova'
    )

    create = PostgresOperator(
        task_id="create_person",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="""
    create table if not exists test_table_volkova(id int)
    """
    )

    add = PostgresOperator(
        task_id="add",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="""
    insert into test_table_volkova(id) values(100);  commit;
    """
    )

    check = PythonOperator(
        task_id="check",
        python_callable=is_odd,
        op_kwargs={"num": take_num}
    )

drop >> create >> add >> check