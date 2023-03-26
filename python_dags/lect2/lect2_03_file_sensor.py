# macros, template, sensor
from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta


default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=2)
}

with DAG(
        dag_id="file_sensor",
        start_date=datetime(2023, 1, 15),
        description="file_sensor",
        default_args=default_args,
        schedule_interval=None,
        tags=["lect2"]
) as dag:

    # оператор FileSensor по дефолту использует conn = fs_default
    file_sensor = FileSensor(
        task_id="file_sensor",
        filepath="{{ var.value.file_to_sensor }}", # file_to_sensor,
        poke_interval=5,
        timeout=80
    )

file_sensor
