from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.models.variable import Variable
from time import sleep
import os

# vars
airflow_cfg = Variable.get("airflow.cfg")
string_with_param_load_example = Variable.get("string_with_params_load_example")


def check_and_edit_config_with_logging(log_path):
    """

    :param path_conf_txt: темповый конфиг, который можно редактировать
    :param log_path: путь для генерации лог файла

    """

    os.system(f"cat {airflow_cfg} |grep load_examples > {string_with_param_load_example}")

    with open(f"{string_with_param_load_example}") as file:

        for line in file:
            config_to_check = line

    if "load_examples = True" in config_to_check:
        os.system(f"echo load_examples = True SUCCESS >> {log_path}")
    else:
        os.system(f"echo load_examples ERROR >> {log_path}")
        os.system(f"echo ... change_config ... >> {log_path}")
        sleep(5)
        os.system(f"sed -i 's/amples = False/amples = True/g' {airflow_cfg}")
        os.system(f"echo load_examples = True SUCCESS >> {log_path}")


default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
    "1_3_check_config",
    start_date=datetime(2023, 1, 15),
    default_args=default_args,
    description="Запускает проверку эйрфлоу конфига, редактирует конфиг, собирает логи",
    schedule_interval=None,
    tags=["airflow_practice"]
)

start = BashOperator(
    task_id="start",
    bash_command="sleep 5",
    dag=dag
)

check_conf = PythonOperator(
    task_id="check_config",
    python_callable=check_and_edit_config_with_logging,
    op_kwargs={"log_path": "/tmp/log_check_airflow_dag.txt"},
    dag=dag
)

start >> check_conf
