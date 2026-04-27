import datetime
import requests
import json
import os
import pandas as pd
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator


# New helper to fetch from your FastAPI service
def fetch_from_api(endpoint):
    api_url = f"http://user-product-api:8000/{endpoint}"
    response = requests.get(api_url)
    response.raise_for_status()
    data = response.json()
    
    # Define paths
    base_dir = "/opt/airflow/data"
    os.makedirs(base_dir, exist_ok=True)
    
    csv_path = f"{base_dir}/{endpoint}.csv"
    
    df = pd.DataFrame(data)
    if not df.empty:
        df.to_csv(csv_path, index=False)
        print(f"Successfully saved {len(df)} items to {csv_path}")
    else:
        print(f"Warning: API returned no data for {endpoint}.")

# Define the targets
TARGET_IDS = ['products', 'users']

with DAG(
    'api_extraction_dag',
    start_date=datetime.datetime(2026, 4, 25),
    schedule='0 0 * * *',
    catchup=False,
    tags=['api', 'extraction']
) as dag:

    for ds_id in TARGET_IDS:
        # Task 1: Call our local FastAPI instead of Kaggle
        t1 = PythonOperator(
            task_id=f'extract_csv_{ds_id}',
            python_callable=fetch_from_api, # Point to the new function
            op_kwargs={'endpoint': ds_id},
            retries=3,
            retry_delay=datetime.timedelta(minutes=2),
            retry_exponential_backoff=True
        )
        
        # Task 2: Trigger downstream processing
        t2 = TriggerDagRunOperator(
            task_id=f'trigger_transform_load_{ds_id}',
            trigger_dag_id='transform_load_dag',
            conf={'ds_id': ds_id},
            wait_for_completion=True
        )
        
        t1 >> t2