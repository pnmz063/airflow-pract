from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from datetime import datetime
# from airflow.models.variable import Variable

# file_to_sensor = Variable.get("file_to_sensor")

random_num = {{ 'macros.random.randint(1, 99)'}}

with DAG(
        dag_id="2_2_file_sensor_dag",
        start_date=datetime(2023, 1, 15),
        description="2_2_file_sensor_dag",
        schedule_interval=None,
        tags=["airflow_practice"]
) as dag:

    # оператор FileSensor по дефолту использует conn = fs_default
    file_sensor = FileSensor(
        task_id="file_sensor",
        filepath="{{ var.value.file_to_sensor }}", # file_to_sensor,
        poke_interval=5,
        timeout=80
    )

file_sensor
