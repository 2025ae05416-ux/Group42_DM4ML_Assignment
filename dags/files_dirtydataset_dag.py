from airflow import DAG
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys



# Ensure dags folder is in path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Updated to include all 4 datasets
TARGET_IDS = ['orders', 'reviews', 'users', 'products']

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

def _generate_all_dirtydatasets(**kwargs):
    from dags.utils.generate_dirty_syntheticdata import generate_all_dirtydatasets
    return generate_all_dirtydatasets()

def _validate_wrapper(ds_id, **kwargs):
    from dags.task_utils import validate_wrapper
    return validate_wrapper(ds_id=ds_id, **kwargs)

def _prepare_wrapper(ds_id, **kwargs):
    from dags.task_utils import prepare_wrapper
    return prepare_wrapper(ds_id=ds_id, **kwargs)

with DAG(
    'file_dirtydataset_dag',
    start_date=datetime(2026, 4, 25),
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    tags=['ingestion', 'synthetic', 'dirty_data'],
) as dag:

    # Task 1: Generate the dirty files
    create_files = PythonOperator(
        task_id='generate_synthetic_files',
        python_callable=_generate_all_dirtydatasets
    )

    # Loop to create parallel branches for each dataset
    for ds_id in TARGET_IDS:

        # Task 2: Sensor waits for the specific CSV
        wait_for_file = FileSensor(
            task_id=f'wait_for_{ds_id}',
            filepath=f'{ds_id}.csv',
            fs_conn_id='fs_default',
            poke_interval=5,
            timeout=600,
            mode='reschedule'
        )

        # Task 3: Quality Check (Pandera validation)
        validate = PythonOperator(
            task_id=f'validate_{ds_id}',
            python_callable=_validate_wrapper,
            op_kwargs={'ds_id': ds_id}
        )

        # Task 4: Cleaning & IST pathing
        prepare = PythonOperator(
            task_id=f'prepare_{ds_id}',
            python_callable=_prepare_wrapper,
            op_kwargs={'ds_id': ds_id}
        )

        # Task 5: Move to Downstream Processing
        trigger_downstream = TriggerDagRunOperator(
            task_id=f'trigger_downstream_{ds_id}',
            trigger_dag_id='transform_load_train_dag',
            conf={'ds_id': ds_id},
            wait_for_completion=False # Set to True if you want to see logs here
        )

        # Execution Flow: 
        # All sensors wait until generator is done. 
        # Then each branch proceeds independently.
        create_files >> wait_for_file >> validate >> prepare >> trigger_downstream