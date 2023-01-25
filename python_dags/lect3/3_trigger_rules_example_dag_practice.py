from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=3)
}


with DAG(
        dag_id="2_3_trigger_rules_dag",
        start_date=datetime(2023, 1, 15),
        description="2_3_trigger_rules_dag",
        default_args=default_args,
        schedule_interval=None,
        tags=["airflow_practice", "lect2"]
) as dag:
    # оператор FileSensor по дефолту использует conn = fs_default
    file_sensor = FileSensor(
        task_id="file_sensor",
        filepath="{{ var.value.file_to_sensor }}",
        poke_interval=2,
        timeout=6
    )

    all_done = BashOperator(
        task_id="all_done",
        bash_command="echo All Done",
        trigger_rule="all_done"
    )

    all_failed = BashOperator(
        task_id="all_failed",
        bash_command="echo All Failed",
        trigger_rule="all_failed"
    )

    none_failed = BashOperator(
        task_id="none_failed",
        bash_command="echo None Failed",
        trigger_rule="none_failed"
    )

    one_failed = BashOperator(
        task_id="one_failed",
        bash_command="echo One Failed",
        trigger_rule="one_failed"
    )

[file_sensor, all_done] >> one_failed >> none_failed >> all_failed
# none_failed?