from airflow.operators.bash import BashOperator
from airflow import DAG
from airflow.utils.dates import days_ago

dag = DAG(
    start_date=days_ago(2),
    dag_id="new_dag",
    schedule=None,
    description="echo string",
    tags=['lect1']
)

start = BashOperator(
    task_id="start",
    bash_command="sleep 2",
    dag=dag
)

echo_string = BashOperator(
    task_id="echo_string",
    bash_command="echo NEW_DAG",
    dag=dag
)

start >> echo_string