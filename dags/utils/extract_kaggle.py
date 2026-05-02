import os
import shutil
from kaggle.api.kaggle_api_extended import KaggleApi
from utils.config import BASE_DATA_DIR


def extract_fn(kaggle_slug, file_name, **kwargs):

    target_file_path = os.path.join(BASE_DATA_DIR,file_name)

    if os.path.exists(target_file_path):
        return

    ti = kwargs["ti"]

    temp_dir=f"/tmp/kaggle_extract_{ti.dag_id}_{ti.task_id}_{ti.try_number}"
    os.makedirs(temp_dir,exist_ok=True)

    os.environ["KAGGLE_CONFIG_DIR"]="/home/airflow/.kaggle"

    api=KaggleApi()
    api.authenticate()

    api.dataset_download_files(
        kaggle_slug,
        path=temp_dir,
        unzip=True
    )

    for root,_,files in os.walk(temp_dir):
        if file_name in files:
            shutil.move(
                os.path.join(root,file_name),
                target_file_path
            )

    shutil.rmtree(temp_dir)