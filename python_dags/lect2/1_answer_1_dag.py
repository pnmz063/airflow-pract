from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pandas as pd
import os

dag_name = "answer1"
default_args = {
    'start_date': datetime(2023, 1, 15),
    'retry_delay': timedelta(minutes=5),
    'retries': 2
}


def download_save_dataframe(some_link, path_to_save, filename):
    df = pd.read_csv(some_link)
    df.to_csv(path_to_save + filename)


def remove_spaces_in_cols(path_to_folder):
    for root, dirs, files in os.walk(path_to_folder):
        for filename in files:
            if str(filename).endswith("csv"):
                print(f"{path_to_folder + filename} stripping...\n")
                df = pd.read_csv(path_to_folder + filename)
                df.columns = list(map(str.strip, list(df)))
                df.to_csv(path_to_folder + filename + "_stripped")
                print(f"{filename} strip SUCCESS")


def count_lines(path_to_folder, filename):
    df = pd.read_csv(path_to_folder + filename)
    print("Number of lines:- " + str(len(df)))


dag = DAG(
    dag_name,
    default_args=default_args,
    description="remove_spaces",
    schedule_interval=None,
    tags=["answers"]
)

download_link = "{{ var.value.download_link }}"
folder_path = "{{ var.value.path_to_folder_KartashovAP }}"


create_dir = BashOperator(
    task_id="create_dir",
    bash_command="{{ var.value.mk_dirs_KartashovAP }}",
    dag=dag
)

download_csv = PythonOperator(
    task_id="download_csv",
    python_callable=download_save_dataframe,
    op_kwargs={
        "some_link": download_link,
        "path_to_save": "{{ var.value.path_to_folder_KartashovAP }}",
        "filename": "got.csv"
    },
    dag=dag
)


rm_spaces = PythonOperator(
    task_id="rm_spaces",
    python_callable=remove_spaces_in_cols,
    op_args=[folder_path],
    dag=dag
)

cnt_lines = PythonOperator(
    task_id="cnt_lines",
    python_callable=count_lines,
    op_args=[folder_path, "got.csv"],
    dag=dag
)

copy_to_local = BashOperator(
    task_id="copy_to_local",
    bash_command="{{ var.value.scp_cmd_KartashovAP }}",
    dag=dag
)

create_dir >> download_csv >> [cnt_lines, rm_spaces] >> copy_to_local
