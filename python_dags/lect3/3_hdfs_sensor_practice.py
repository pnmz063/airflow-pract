from airflow import DAG
from datetime import datetime
from airflow.providers.apache.hdfs.sensors.hdfs import HdfsSensor


with DAG(
    dag_id="3_1_hdfs_file_sensor",
    start_date=datetime(2022, 9, 16),
    schedule_interval=None,
    description="sensor file from hdfs",
    tags=['airflow_practice']
) as dag:

    hdfs_test_conn = HdfsSensor(
        task_id="hdfs_test_conn",
        hdfs_conn_id="hdfs_default",
        filepath="/staging/test.file"
    )

    hdfs_test_conn