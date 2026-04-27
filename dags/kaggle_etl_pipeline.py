import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

# --- 1. LIGHTWEIGHT CONFIGURATION ---
# Keep only basic strings/dicts here. 
# Avoid importing Pandera or creating schema objects at this level.
DATASET_CONFIG = [
    {
        "id": "products",
        "kaggle_slug": "abhayayare/e-commerce-dataset",
        "file_name": "products.csv",
        "target_schema": "dmml_assignment",
        "target_table": "products",
        "pk": "product_id"
    },
    {
        "id": "users",
        "kaggle_slug": "abhayayare/e-commerce-dataset",
        "file_name": "users.csv",
        "target_schema": "dmml_assignment",
        "target_table": "users",
        "pk": "user_id"
    }
]

DATA_PATH = '/opt/airflow/data'

# --- 2. TASK FUNCTIONS (With Local Imports) ---

def extract_fn(kaggle_slug, **kwargs):
    import os
    import json
    import zipfile
    import pandas as pd
    from kaggle.api.kaggle_api_extended import KaggleApi

    # 1. Unique directory for isolation
    ti = kwargs['ti']
    unique_kaggle_dir = f"/tmp/.kaggle_{ti.task_id}_{ti.try_number}"
    os.makedirs(unique_kaggle_dir, exist_ok=True)
    os.environ['KAGGLE_CONFIG_DIR'] = unique_kaggle_dir
    
    # 2. Setup Credentials
    username = os.environ.get('KAGGLE_USERNAME')
    key = os.environ.get('KAGGLE_KEY')
    config_file = os.path.join(unique_kaggle_dir, 'kaggle.json')
    with open(config_file, 'w') as f:
        json.dump({"username": username, "key": key}, f)
    os.chmod(config_file, 0o600)
    
    # 3. Authenticate
    api = KaggleApi()
    api.authenticate()
    
    # 4. Idempotency Check: Don't download if data is already there
    # Note: Adjust 'ecommerce_dataset' if your files are in a different subfolder
    target_folder = os.path.join(DATA_PATH, 'ecommerce_dataset')
    if os.path.exists(target_folder) and len(os.listdir(target_folder)) > 0:
        print(f"DEBUG: Data already exists at {target_folder}. Skipping download.")
    else:
        print(f"DEBUG: Starting download for slug: {kaggle_slug}")
        # Download as ZIP, do not unzip via API to avoid file size mismatches
        api.dataset_download_files(kaggle_slug, path=DATA_PATH, unzip=False)
        
        # Manually extract
        zip_path = [os.path.join(DATA_PATH, f) for f in os.listdir(DATA_PATH) if f.endswith('.zip')][0]
        print(f"DEBUG: Manually extracting {zip_path}")
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(DATA_PATH)
        os.remove(zip_path)

    # 5. Preview & Verification (Look inside the extracted subfolder)
    print(f"DEBUG: Inspecting files in {target_folder}")
    for file in os.listdir(target_folder):
        if file.endswith('.csv'):
            file_path = os.path.join(target_folder, file)
            size_mb = os.path.getsize(file_path) / (1024 * 1024)
            print(f"DEBUG: Found {file} | Size: {size_mb:.2f} MB")
            try:
                df = pd.read_csv(file_path, nrows=5)
                print(f"--- Preview of {file} ---\n{df.to_string()}")
            except Exception as e:
                print(f"ERROR: Could not read {file}: {e}")

def transform_fn(file_name, ds_id, **kwargs):
    import pandas as pd
    import os
    import pandera as pa
    from datetime import datetime

    # Point to the sub-folder where Kaggle extracted the files
    SUB_FOLDER = os.path.join(DATA_PATH, 'ecommerce_dataset')
    file_path = os.path.join(SUB_FOLDER, file_name)
    
    # INDENTATION FIX: Ensure the block below is indented by 4 spaces
    if not os.path.exists(file_path):
        available_files = os.listdir(SUB_FOLDER) if os.path.exists(SUB_FOLDER) else "Folder missing"
        raise FileNotFoundError(f"File not found at {file_path}. Available: {available_files}")
    
    df = pd.read_csv(file_path)
    df.columns = [c.lower().replace(' ', '_') for c in df.columns]
    
    if 'signup_date' in df.columns:
        df['signup_date'] = pd.to_datetime(df['signup_date'], dayfirst=True)

    # Define schemas LOCALLY inside the task
    schemas = {
        "products": pa.DataFrameSchema({
            "product_id": pa.Column(str, pa.Check.str_startswith("P")),
            "price": pa.Column(float, pa.Check.ge(0)),
            "rating": pa.Column(float, pa.Check.in_range(0, 5))
        }),
        "users": pa.DataFrameSchema({
            "user_id": pa.Column(str, pa.Check.str_startswith("U")),
            "email": pa.Column(str, pa.Check.str_contains("@")),
            "signup_date": pa.Column(datetime)
        })
    }

    validated_df = schemas[ds_id].validate(df, lazy=True)
    
    out_path = f"{DATA_PATH}/transformed_{ds_id}.csv"
    validated_df.to_csv(out_path, index=False)

def load_fn(target_schema, target_table, pk, ds_id, **kwargs):
    import pandas as pd
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    df = pd.read_csv(f"{DATA_PATH}/transformed_{ds_id}.csv")
    
    rows = list(df.itertuples(index=False, name=None))
    postgres_hook.insert_rows(
        table=f"{target_schema}.{target_table}",
        rows=rows,
        target_fields=df.columns.tolist(),
        replace=True,
        replace_index=pk
    )

# --- 3. DAG DEFINITION ---

with DAG(
    'generalized_kaggle_etl',
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    doc_md="Optimized thin DAG to prevent import timeouts."
) as dag:

    for config in DATASET_CONFIG:
        ds_id = config['id']

        t1 = PythonOperator(
            task_id=f'extract_{ds_id}',
            python_callable=extract_fn,
            op_kwargs={'kaggle_slug': config['kaggle_slug']}
        )

        t2 = PythonOperator(
            task_id=f'transform_{ds_id}',
            python_callable=transform_fn,
            op_kwargs={
                'file_name': config['file_name'], 
                'ds_id': ds_id
            }
        )

        t3 = PythonOperator(
            task_id=f'load_{ds_id}',
            python_callable=load_fn,
            op_kwargs={
                'target_schema': config['target_schema'],
                'target_table': config['target_table'],
                'pk': config['pk'],
                'ds_id': ds_id
            }
        )

        t1 >> t2 >> t3