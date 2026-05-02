from airflow import DAG
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.standard.sensors.time_delta import TimeDeltaSensor
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id='scheduler_api_file_dag',
    default_args=default_args,
    start_date=datetime(2026, 5, 1),
    schedule='0 * * * *',  # Runs every hour at minute 0
    catchup=False,
    tags=['master_orchestrator']
) as dag:

    # 1. Trigger the API Extraction Child DAG
    trigger_api_child = TriggerDagRunOperator(
        task_id='call_api_extraction_dag',
        trigger_dag_id='api_extraction_dag', 
        wait_for_completion=True,           # Wait for API tasks to finish before starting the timer
        poke_interval=20
    )

    # 2. The 2-minute delay dependency
    wait_two_mins = TimeDeltaSensor(
        task_id='wait_2_minutes',
        delta=timedelta(minutes=2),
    )

    # 3. Trigger the File Generation Child DAG
    trigger_file_gen_child = TriggerDagRunOperator(
        task_id='call_file_generation_dag',
        trigger_dag_id='file_trigger_dag', # Ensure this matches your second child's ID
        wait_for_completion=False           # Master can finish once this is triggered
    )

    # Dependency Flow
    trigger_api_child >> wait_two_mins >> trigger_file_gen_child