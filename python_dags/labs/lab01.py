from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pandas as pd
import os


def download_file_and_save_csv(link, path_to_save, your_name):
    df = pd.read_csv(link)
    df.to_csv(f"{path_to_save}{your_name}_got.csv")
    # /tmp/labs/KartashovAP/lab1/KartashovAP_got.csv


def remove_spaces_in_cols(path_to_folder):
    for root, dirs, files in os.walk(path_to_folder):
        for filename in files:
            if str(filename).endswith("csv"):
                print(f"{path_to_folder + filename} stripping...\n")
                df = pd.read_csv(path_to_folder + filename)
                df.columns = list(map(str.strip, list(df)))
                df.to_csv(path_to_folder + filename + "_stripped")
                print(f"{filename} strip SUCCESS")
                # /tmp/labs/KartashovAP/lab1/KartashovAP_got.csv_stripped


def count_lines(path_to_folder, filename):
    df = pd.read_csv(path_to_folder + filename)
    print("Number of lines:- " + str(len(df)))


folder_path = "{{ var.value.path_to_folder_KartashovAP }}"
default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=1)
}

with DAG(
        "answer_1",
        start_date=datetime(2023, 1, 15),
        description="remove_spaces",
        default_args=default_args,
        schedule_interval=None,
        tags=["lab01"]
) as dag:
    create_dir = BashOperator(
        task_id="create_dir",
        bash_command="{{ var.value.mk_dirs_KartashovAP }}"
    )

    download_csv = PythonOperator(
        task_id="download_csv",
        python_callable=download_file_and_save_csv,
        op_kwargs={
            "link": "{{ var.value.download_link }}",
            "path_to_save": folder_path,
            "your_name": "KartashovAP"
        }
    )

    rm_spaces = PythonOperator(
        task_id="rm_spaces",
        python_callable=remove_spaces_in_cols,
        op_args=[folder_path]
    )

    cnt_lines = PythonOperator(
        task_id="cnt_lines",
        python_callable=count_lines,
        op_args=[folder_path, "KartashovAP_got.csv"]
    )

    copy_to_local = BashOperator(
        task_id="copy_to_local",
        bash_command="{{ var.value.copy_to_local_KartashovAP }}"
    )

create_dir >> download_csv >> [cnt_lines, rm_spaces] >> copy_to_local
