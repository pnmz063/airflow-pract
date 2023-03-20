from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago


def is_odd(num):
    if num % 2 == 0:
        print(f"{num} is even")
        return 'even'
    else:
        print(f"{num} is odd")
        return 'odd'


def get_val():
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    get_val_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    list_num = get_val_hook.get_records("select num from lab02 where name = 'kartashovap'")
    print(list_num[0][0])
    return list_num[0][0]


POSTGRES_CONN_ID = "postgres_default"

with DAG(
        "lab02_hook",
        start_date=days_ago(2),
        schedule_interval=None,
        tags=["lab02"]
) as dag:
    psql_hook = PythonOperator(
        task_id="psql_hook",
        python_callable=get_val
    )

    check_num = PythonOperator(
        task_id="check_num",
        python_callable=is_odd,
        op_kwargs={"num": get_val()}
    )

    upsert_table = PostgresOperator(
        task_id='upsert_val',
        sql=f"update lab02 set parity = '{is_odd(get_val())}' where name = 'kartashovap'"
    )

    psql_hook >> check_num >> upsert_table
