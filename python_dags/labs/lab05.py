from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
from random import randint
from pendulum import timezone

local_tz = timezone("Europe/Moscow")


def push_income_expense(ti):
    income_expense_list = [randint(0, 9999), randint(0, 9999)]
    ti.xcom_push(key="push_income_expense", value=income_expense_list)


POSTGRES_CONN_ID = "postgres_default"

with DAG(
        dag_id="lab05",
        start_date=datetime(2023, 1, 13, tzinfo=local_tz),
        description="lab05",
        schedule_interval="@daily",
        tags=["answers"]
) as dag:
    start = BashOperator(task_id="start", bash_command="sleep 5", do_xcom_push=False)

    income_expense_generated = PythonOperator(
        task_id="income_expense_generated",
        python_callable=push_income_expense
    )

    stop_generated = BashOperator(task_id="stop_generated", bash_command="sleep 5",
                                  do_xcom_push=False, trigger_rule="all_success")

    with TaskGroup("create_tables_tasks") as create_tables_tasks:
        start_created = BashOperator(task_id="start_created", bash_command="sleep 5",
                                     do_xcom_push=False)
        create_person = PostgresOperator(
            task_id="create_person",
            postgres_conn_id=POSTGRES_CONN_ID,
            sql="""
                create table if not exists person (id int4, p_name varchar(30), time_key varchar(10))
                """
        )

        create_person_agg_info = PostgresOperator(
            task_id="create_person_agg_info",
            postgres_conn_id=POSTGRES_CONN_ID,
            sql="""
            create table if not exists person_agg_info (id int4, p_name varchar(30), bank_account int4, time_key varchar(10));
            """
        )

        create_person_op = PostgresOperator(
            task_id="create_person_op",
            postgres_conn_id=POSTGRES_CONN_ID,
            sql="""
            create table if not exists person_operation (id int4, income int4, expense int4, time_key varchar(10));
            """
        )

        stop_created = BashOperator(task_id="stop_created", bash_command="echo start; sleep 5",
                                    do_xcom_push=False, trigger_rule="all_success")

        start_created >> [create_person, create_person_op, create_person_agg_info] >> stop_created

    with TaskGroup("insert_values_tasks") as insert_values_tasks:
        start_insert_values = BashOperator(task_id="start_insert", bash_command="echo start_insert; sleep 5",
                                           do_xcom_push=False)
        person_insert = PostgresOperator(
            task_id="insert_person",
            postgres_conn_id=POSTGRES_CONN_ID,
            sql="insert into person values (25091992, 'Kartashov Andrei', '{{ macros.ds_add(ds, -2) }}')"
            # вариант со звездочкой (-2 дня от exec date)
        )

        person_operation_insert = PostgresOperator(
            task_id="insert_person_op",
            postgres_conn_id=POSTGRES_CONN_ID,
            sql=""" 
                insert into person_operation 
                values ( 
                        25091992, 
                        '{{ ti.xcom_pull(task_ids='income_expense_generated', key='push_income_expense')[1] }}'::integer, 
                        '{{ ti.xcom_pull(task_ids='income_expense_generated', key='push_income_expense')[0] }}'::integer,
                        '{{ macros.ds_add(ds, -2) }}'
                )
            """
        )

        person_agg_insert = PostgresOperator(
            task_id="insert_person_agg",
            postgres_conn_id=POSTGRES_CONN_ID,  # local_con - postgres_default
            sql="""
                insert into person_agg_info (id, p_name, bank_account, time_key) 
                select id, p_name, bank_account, time_key 
                from (
                    select 
                        po.id, 
                        p.p_name, 
                        po.time_key, 
                        sum(income - expense) over (partition by po.id, po.time_key) as bank_account 
                    from person_operation po 
                    inner join person p 
                    on p.id = po.id and p.time_key = po.time_key where p.time_key = '{{ macros.ds_add(ds, -2) }}') as tmp 
            
            """
        )

        stop_insert_values = BashOperator(
            task_id="stop_insert_values",
            bash_command="echo stop insert; sleep 5",
            trigger_rule="all_success",
            do_xcom_push=False
        )

        start_insert_values >> [person_insert, person_operation_insert] >> person_agg_insert

    stop = BashOperator(task_id="stop", bash_command="echo stop; sleep 5", do_xcom_push=False)

    start >> income_expense_generated >> stop_generated >> create_tables_tasks >> insert_values_tasks >> stop
