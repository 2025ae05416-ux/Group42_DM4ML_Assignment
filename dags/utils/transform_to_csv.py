import os
import pandas as pd
from dags.utils.config import BASE_DATA_DIR
from dags.utils.preprocess import preprocess_fn


def transform_fn(file_name, ds_id, **kwargs):

    path=os.path.join(BASE_DATA_DIR,file_name)

    df=pd.read_csv(path)

    df=preprocess_fn(df)

    df.to_csv(
        os.path.join(
            BASE_DATA_DIR,
            f"transformed_{ds_id}.csv"
        ),
        index=False
    )