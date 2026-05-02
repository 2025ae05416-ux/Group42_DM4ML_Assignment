from airflow import DAG
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys


sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

TARGET_IDS = ['orders', 'reviews']

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}


def _generate_all_data(**kwargs):
    from dags.utils.file_generator import generate_all_data
    return generate_all_data()

def _validate_wrapper(**kwargs):
    from dags.task_utils import validate_wrapper
    return validate_wrapper(**kwargs)

def _prepare_wrapper(**kwargs):
    from dags.task_utils import prepare_wrapper
    return prepare_wrapper(**kwargs)


with DAG(
    'file_trigger_dag',
    start_date=datetime(2026, 4, 25),
    default_args={'retries': 2, 'retry_delay': timedelta(minutes=1)},
    schedule=None, 
    # schedule='* * * * *',
    max_active_runs=1,
    catchup=False,
    tags=['ingestion', 'file','child'],
) as dag:

    create_files = PythonOperator(
        task_id='generate_synthetic_files',
        python_callable=_generate_all_data
    )

    for ds_id in TARGET_IDS:

         # The Sensor (Looking for the file created by the API)
        wait_for_file = FileSensor(
            task_id=f'wait_for_{ds_id}',
            filepath=f'/opt/airflow/data/{ds_id}.csv',
            fs_conn_id='fs_default',
            poke_interval=20,
            mode='reschedule'
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
            task_id=f'trigger_downstream_{ds_id}',
            trigger_dag_id='transform_load_train_dag',
            conf={'ds_id': ds_id},
            wait_for_completion=True,
            reset_dag_run=True
        )

        create_files >> wait_for_file >> validate >> prepare >> trigger_downstream