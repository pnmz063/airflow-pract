import pandas as pd
import os


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

# folder_path = путь к вашему каталогу
