from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator

with DAG(
        dag_id="2_4_backfill_example_dag",
        start_date=datetime(2023, 1, 13),
        description="2_4_backfill_example_dag",
        schedule_interval="@daily",
        catchup=True,
        tags=["airflow_practice"]
) as dag:

    print_date = BashOperator(
        task_id="print_date",
        bash_command='echo "execution_date = {{ ds }}" >> /tmp/log_backfill_dag.txt'
    )

    print_date
