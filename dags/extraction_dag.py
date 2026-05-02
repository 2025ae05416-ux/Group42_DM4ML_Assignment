import sys
import os
import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

TARGET_IDS = ['products', 'users']

default_args = {
    "retries": 2,
    "retry_delay": datetime.timedelta(minutes=5),
    "execution_timeout": datetime.timedelta(minutes=30),
}


def _fetch_from_api(endpoint, **kwargs):
    import requests
    import pandas as pd
    from dags.utils.metadata_tracker import MetadataTracker

    api_url = f"http://user-product-api:8000/{endpoint}"
    response = requests.get(api_url, timeout=30)  
    response.raise_for_status()
    data = response.json()

    base_dir = "/opt/airflow/data"
    os.makedirs(base_dir, exist_ok=True)
    csv_path = f"{base_dir}/{endpoint}.csv"

    df = pd.DataFrame(data)
    if df.empty:
        print(f"Warning: API returned no data for {endpoint}.")
        return 

    df.to_csv(csv_path, index=False)

    tracker = MetadataTracker(csv_path, user_id="api_extraction_service")
    tracker.add_source(api_url)
    tracker.log_transformation("api_fetch", f"Extracted {len(df)} rows from API")
    tracker.save()

    print(f"Successfully saved {len(df)} items and metadata to {csv_path}")


def _validate_wrapper(**kwargs):
    from dags.task_utils import validate_wrapper
    return validate_wrapper(**kwargs)


def _prepare_wrapper(**kwargs):
    from dags.task_utils import prepare_wrapper
    return prepare_wrapper(**kwargs)


with DAG(
    'api_extraction_dag',
    start_date=datetime.datetime(2026, 4, 25),
    default_args=default_args,
    schedule=None,  # Triggered hourly by Master DAG
    max_active_runs=1,
    catchup=False,
    tags=['api', 'extraction', 'child']
) as dag:

    for ds_id in TARGET_IDS:

        extract = PythonOperator(
            task_id=f'extract_csv_{ds_id}',
            python_callable=_fetch_from_api,
            op_kwargs={'endpoint': ds_id}
        )

        validate = PythonOperator(
            task_id=f'validate_{ds_id}',
            python_callable=_validate_wrapper,
            op_kwargs={'ds_id': ds_id}
        )

        prepare = PythonOperator(
            task_id=f'prepare_{ds_id}',
            python_callable=_prepare_wrapper,
            op_kwargs={'ds_id': ds_id}
        )

        trigger_downstream = TriggerDagRunOperator(
            task_id=f'trigger_transform_load_{ds_id}',
            trigger_dag_id='transform_load_train_dag',
            conf={'ds_id': ds_id},
            wait_for_completion=True,
            reset_dag_run=True
        )


        extract >> validate >> prepare >> trigger_downstream