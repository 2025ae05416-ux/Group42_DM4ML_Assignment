from airflow import DAG
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys

from file_generator import generate_all_data
# Ensure the project root is on sys.path so file_generator can be imported
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))



TARGET_IDS = ['orders', 'reviews']

with DAG(
    'file_trigger_dag',
    start_date=datetime(2026, 4, 25),
    schedule='@daily',
    catchup=False
) as dag:

    # New Task: Create files before checking for them
    create_files = PythonOperator(
        task_id='generate_synthetic_files',
        python_callable=generate_all_data
    )

    for ds_id in TARGET_IDS:
        wait_for_file = FileSensor(
            task_id=f'wait_for_{ds_id}',
            filepath=f'{ds_id}.csv', 
            fs_conn_id='fs_default', 
            poke_interval=30
        )

        trigger_worker = TriggerDagRunOperator(
            task_id=f'trigger_worker_{ds_id}',
            trigger_dag_id='transform_load_dag',
            conf={'ds_id': ds_id},
            wait_for_completion=True
        )

        # Flow: Create -> Wait -> Trigger
        create_files >> wait_for_file >> trigger_worker