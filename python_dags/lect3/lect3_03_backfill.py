# depends_on_past

from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator
from datetime import timedelta
from pendulum import timezone

local_tz = timezone("Europe/Moscow")

default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=1)
}

with DAG(
        dag_id="3_backfill",
        start_date=datetime(2023, 1, 13, tzinfo=local_tz),
        description="2_4_backfill_example_dag",
        default_args=default_args,
        schedule_interval="@daily",
        catchup=True,
        tags=["lect3"]
) as dag:

    print_date = BashOperator(
        task_id="print_date",
        bash_command='echo "execution_date = {{ ds }}" >> /tmp/log_backfill_dag.txt'
    )

    print_date
