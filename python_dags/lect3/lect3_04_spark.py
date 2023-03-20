from airflow import DAG
from datetime import datetime
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta

SPARK_MAIN_CONF = {
    'master': 'yarn',
    'spark.submit.deployMode': 'cluster',
    'spark.dynamicAllocation.shuffleTracking.enabled': 'true',
    'spark.dynamicAllocation.enabled': 'true',
    'spark.dynamicAllocation.maxExecutors': '100',
    'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
    # и так далее
}

with DAG(
    "spark",
    default_args={},
    description="",
    max_active_runs=1,
    tags=['lect3']
) as dag:
    app = SparkSubmitOperator(
        name="sparkSubmitDag",
        conn_id="spark_op",
        application="path_to_jar", # если вы используете scala
        conf={
           **SPARK_MAIN_CONF,
            'spark.your_package.POSTGRES_URL': 'POSTGRES_URL',
            'spark.your_package.DATE': '{{ ds }}',
            # и так далее
        },
        java_class='your_main_class',
        queue='prod',
        jars='your_postgres_driver_jar',
        driver_memory='5g',
        executor_memory='8g',
        executor_cores=5
    )