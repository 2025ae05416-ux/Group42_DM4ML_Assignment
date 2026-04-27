from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from utils import feature_engineering_fn, load_structured_fn, plot_fn, transform_fn, load_fn, archive_file, DATASET_CONFIG

# Helper to look up config by ID
def get_config_by_id(ds_id):
    return next((item for item in DATASET_CONFIG if item["id"] == ds_id), None)

with DAG(
    'transform_load_dag',
    start_date=datetime(2026, 4, 25),
    schedule=None,
    catchup=False
) as dag:

    def transform_wrapper(**kwargs):
        ds_id = kwargs['dag_run'].conf.get('ds_id')
        config = get_config_by_id(ds_id)
        
        # ADDED: Defensive check
        if config is None:
            raise ValueError(f"No configuration found for ds_id: {ds_id}. Check DATASET_CONFIG.")
            
        print(f"DEBUG: Processing {ds_id} using {config['file_name']}")
        transform_fn(file_name=config['file_name'], ds_id=ds_id)

    def load_wrapper(**kwargs):
        ds_id = kwargs['dag_run'].conf.get('ds_id')
        config = get_config_by_id(ds_id)
        
        # ADDED: Defensive check
        if config is None:
            raise ValueError(f"No configuration found for ds_id: {ds_id}.")
            
        load_fn(
            target_schema=config['target_schema'],
            target_table=config['target_table'],
            pk=config['pk'],
            ds_id=ds_id
        )
        archive_file(file_name=config['file_name'], ds_id=ds_id)
        
    def plot_wrapper(**kwargs):
        ds_id = kwargs['dag_run'].conf.get('ds_id')
        config = get_config_by_id(ds_id)
        # Pass the specific transformed file name
        # Assuming your transform_fn creates: f"transformed_{ds_id}.csv"
        transformed_file = f"transformed_{ds_id}.csv"
        plot_fn(ds_id=ds_id)

    def feature_wrapper(**kwargs):
        ds_id = kwargs['dag_run'].conf.get('ds_id')

       # if ds_id not in ['orders', 'reviews']:
       #     print(f"DEBUG: Skipping feature engineering for {ds_id} (No interactions found).")
       #     return        
        valid_ds = [
        'orders',
        'reviews',
        'users',
        'products'
        ]

        if ds_id not in valid_ds:
            raise ValueError(
                f"Unsupported ds_id: {ds_id}"
            )
        feature_engineering_fn(ds_id)

    # New task
    t4 = PythonOperator(
        task_id='engineer_features',
        python_callable=feature_wrapper
    )

    t1 = PythonOperator(
        task_id='transform_dataset',
        python_callable=transform_wrapper
    )

    t3 = PythonOperator(
        task_id='load_dataset',
        python_callable=load_wrapper
    )

    t2 = PythonOperator(
        task_id='generate_plots',
        python_callable=plot_wrapper
    )
    # 1. Define the Feature Loading Task
    t5 = PythonOperator(
        task_id='load_features',
        python_callable=load_structured_fn, # Your custom feature-loading logic
        op_kwargs={'ds_id': '{{ dag_run.conf.ds_id }}'}
    )

    # UPDATED: Defining the linear dependency
    # T1 (transform) -> T2 (plot) -> T4 (engineer) -> T5 (load features) -> T3 (load raw)
    t1 >> t2 >> t4 >> t5 >> t3