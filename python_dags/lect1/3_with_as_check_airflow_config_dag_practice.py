from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from time import sleep
import os


def check_config(path_conf_txt, log_path):
    os.system(f"cat {path_conf_txt} |grep load_examples > /tmp/load_example_value.txt")

    with open("/tmp/load_example_value.txt") as file:
        for line in file:
            load_examples = line

    if "load_examples = True" in load_examples:
        os.system(f"echo load_examples = True SUCCESS >> {log_path}")
    else:
        os.system(f"echo load_examples ERROR >> {log_path}")
        os.system(f"echo ... change_config ... >> {log_path}")
        sleep(5)
        os.system(f"sed -i 's/amples = False/amples = True/g' {path_conf_txt}")
        os.system(f"echo load_examples = True SUCCESS >> {log_path}")


with DAG(
        "1_3_1_check_config_with_as_set_upstream",
        start_date=datetime(2023, 1, 15),
        description="Запускает проверку эйрфлоу конфига",
        schedule_interval=None,
        tags=["airflow_practice"]
) as dag:
    cat_conf = BashOperator(
        task_id="cat_config",
        bash_command="cat /opt/airflow/airflow.cfg > /tmp/config.txt"
    )

    check_conf = PythonOperator(
        task_id="check_conf",
        python_callable=check_config,
        op_kwargs={"path_conf_txt": "/tmp/config.txt", "log_path": "/tmp/log_check_airflow_dag.txt"},
    )

cat_conf.set_downstream(check_conf)
