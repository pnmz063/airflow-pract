from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.models.variable import Variable
from time import sleep
import os

# vars
airflow_cfg = Variable.get("airflow.cfg")
string_with_param_load_example = Variable.get("kartashov_string_with_params_load_example")

"""решение со звездочкой"""


def check_and_edit_config_with_logging(log_path):
    """
    :param log_path: путь для генерации лог файла
    """

    os.system(f"mkdir -p /opt/airflow/dev/kartashov/lab01; "
              f"cp {airflow_cfg} /opt/airflow/dev/kartashov/lab01/config.cfg")
    os.system(f"cat {airflow_cfg} |grep load_examples > {string_with_param_load_example}")

    with open(f"{string_with_param_load_example}") as file:
        for line in file:
            config_to_check = line

            if "load_examples = False" in config_to_check:
                os.system(f"echo load_examples = False SUCCESS >> {log_path}")
            else:
                os.system(f"echo load_examples ERROR >> {log_path}")
                os.system(f"echo ... change_config ... >> {log_path}")
                sleep(5)
                os.system(f"sed -i 's/amples = True/amples = False/g' /opt/airflow/dev/kartashov/lab01/config.cfg")
                os.system(f"cat /opt/airflow/dev/kartashov/lab01/config.cfg |grep load_examples >> {log_path}")


default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
    "lab01_1_check_config",
    start_date=datetime(2023, 1, 15),
    default_args=default_args,
    description="Запускает проверку эйрфлоу конфига, редактирует конфиг, собирает логи",
    schedule_interval=None,
    tags=["lab01_1"]
)

start = BashOperator(
    task_id="start",
    bash_command="sleep 5",
    dag=dag
)

check_conf = PythonOperator(
    task_id="check_config",
    python_callable=check_and_edit_config_with_logging,
    op_kwargs={"log_path": "/opt/airflow/dev/kartashov/lab01/log_check_airflow_dag.txt"},
    dag=dag
)

start >> check_conf

# """решение без звездочки"""
# cp_op = BashOperator( task_id="cp_op", bash_command="mkdir -p
# /opt/airflow/dev/kartashov/lab01; cp /opt/airflow/airflow.cfg /opt/airflow/dev/kartashov/lab01/config.cfg", dag=dag )
#
# cat_param = BashOperator(
#     task_id="cat_param",
#     bash_command="cat /opt/airflow/dev/kartashov/lab01/config.cfg | grep load_examples",
#     dag=dag
# )
