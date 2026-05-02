import os
from dags.utils.structured_load import load_structured_fn

def get_config_by_id(ds_id):
    from dags.utils.config import DATASET_CONFIG, BASE_DATA_DIR, DATA_FILE_MAP

    return next((item for item in DATASET_CONFIG if item["id"] == ds_id), None)

# -------------------------
# TRANSFORM (Consumes Prepared Data)
# -------------------------
def transform_wrapper(**kwargs):
    from dags.utils.config import DATASET_CONFIG, BASE_DATA_DIR, DATA_FILE_MAP
    from dags.utils.metadata_tracker import MetadataTracker
    from dags.utils.transform_to_csv import transform_fn

    ti = kwargs["ti"]
    ds_id = kwargs["dag_run"].conf.get("ds_id")
    config = get_config_by_id(ds_id)

    if config is None:
        raise ValueError(f"No config found for ds_id={ds_id}")

    # 1. NEW LOGIC: Define the path to the PREPARED data from the Ingestion DAG
    # We use the 'prepared_' prefix established in the preparation_wrapper
    prepared_file_path = os.path.join(BASE_DATA_DIR, f"prepared_{ds_id}.csv")
    
    if not os.path.exists(prepared_file_path):
        raise FileNotFoundError(f"Prepared file not found at {prepared_file_path}. Ingestion DAG might have failed.")

    # 2. Setup paths and tracker
    output_path = os.path.join(BASE_DATA_DIR, f"transformed_{ds_id}.csv")
    tracker = MetadataTracker(output_path, user_id="airflow_svc_account")
    tracker.add_source(f"prepared_{ds_id}.csv") # Track the prepared file as source

    # 3. Perform transformation on the CLEANED data
    transform_fn(file_name=f"prepared_{ds_id}.csv", ds_id=ds_id)

    # 4. Log steps
    tracker.log_transformation("transform_fn", "Transformation applied to validated/prepared data")
    tracker.save()
    print(f"Transformation completed for prepared dataset: {ds_id}")

# -------------------------
# PLOTS (Visual Quality Check)
# -------------------------
def plot_wrapper(**kwargs):
    from dags.utils.plotting import plot_fn
    """
    Generates visualizations and PDF reports. 
    Now serves as a visual audit of the prepared and transformed data.
    """
    ds_id = kwargs["dag_run"].conf.get("ds_id")
    
    # plot_fn internal logic (plotting.py) already handles quality stats
    # It will now reflect the 'prepared' state of the data
    plot_fn(ds_id=ds_id)

# -------------------------
# FEATURE ENGINEERING
# -------------------------
def feature_wrapper(**kwargs):
    from dags.utils.feature_engineering import feature_engineering_fn

    ds_id = kwargs["dag_run"].conf.get("ds_id")
    valid_ds = ["orders", "reviews", "users", "products"]

    if ds_id not in valid_ds:
        raise ValueError(f"Unsupported ds_id: {ds_id}")

    # This now runs on data that has passed validation/preparation checks
    feature_engineering_fn(ds_id)

# -------------------------
# LOAD RAW TABLES
# -------------------------
def load_wrapper(**kwargs):
    from dags.utils.load_raw_db import load_fn
    from dags.utils.archive_files import archive_file
    
    ti = kwargs["ti"]
    ds_id = kwargs["dag_run"].conf.get("ds_id")
    config = get_config_by_id(ds_id)
    metrics_data = ti.xcom_pull(task_ids="train_model_task")

    # Primary Database Load
    load_fn(
        target_schema=config["target_schema"],
        target_table=config["target_table"],
        pk=config["pk"],
        ds_id=ds_id,
    )

    # Archive the PREPARED file instead of the RAW file to keep record of what was loaded
    prepared_filename = f"prepared_{ds_id}.csv"
    archive_file(file_name=config["file_name"], ds_id=ds_id)

def prepare_wrapper(ds_id, **kwargs):
    import pandas as pd
    import os
    from dags.utils.preparation import prepare_data_fn
    from dags.utils.config import BASE_DATA_DIR
    from dags.task_utils import get_config_by_id

    config = get_config_by_id(ds_id)
    if config is None:
        raise ValueError(f"No config found for ds_id={ds_id}")

    # Read raw file, not transformed/feature file
    file_path = BASE_DATA_DIR / config["file_name"]

    if not file_path.exists():
        raise FileNotFoundError(f"Raw file not found: {file_path}")

    df = pd.read_csv(file_path)

    # Apply cleaning
    df_clean = prepare_data_fn(df, ds_id)

    # Save as prepared version for the transform task
    prep_path = os.path.join(BASE_DATA_DIR, f"prepared_{ds_id}.csv")
    df_clean.to_csv(prep_path, index=False)

    print(f"Preparation complete. Cleaned file saved to: {prep_path}")

    # Push path to XCom for next task
    kwargs['ti'].xcom_push(key='prepared_file_path', value=prep_path)

def validate_wrapper(ds_id, **kwargs):
    import pandas as pd
    from dags.utils.validation import validate_data
    from dags.utils.config import BASE_DATA_DIR
    from dags.task_utils import get_config_by_id

    config = get_config_by_id(ds_id)
    if config is None:
        raise ValueError(f"No config found for ds_id={ds_id}")

    file_path = BASE_DATA_DIR / config["file_name"]

    if not file_path.exists():
        raise FileNotFoundError(f"Expected file not found: {file_path}")

    # Peek at columns first, then parse dates only if the column exists
    peek = pd.read_csv(file_path, nrows=0)
    date_cols = [col for col in ["review_date", "order_date", "signup_date"] if col in peek.columns]

    df = pd.read_csv(file_path, parse_dates=date_cols if date_cols else False)

    stats, errors = validate_data(df, ds_id)

    if errors:
        raise ValueError(f"Validation failed for {ds_id}: {errors}")

    print(f"Validation passed for {ds_id}: {stats}")

def load_features_wrapper(**kwargs):
    from dags.utils.structured_load import load_structured_fn
    
    ds_id = kwargs["dag_run"].conf.get("ds_id")

    # Optional skip for datasets without structured features
    if ds_id not in ["reviews"]:
        print(f"Skipping structured feature load for {ds_id}")
        return

    load_structured_fn(ds_id)

def train_model_wrapper(**kwargs):
    
    """
    Wrapper to trigger MLflow model training and return metrics via XCom.
    """
    import os
    import datetime
    from zoneinfo import ZoneInfo
    from dags.utils.model_training import train_and_evaluate
    from dags.utils.config import DATA_FILE_MAP
    
    # Get Dataset ID from the DagRun configuration
    ds_id = kwargs["dag_run"].conf.get("ds_id")
    ist_tz = ZoneInfo("Asia/Kolkata")

    print(f"Starting model training task for: {ds_id} at {datetime.datetime.now(ist_tz)}")

    # 1. Guard Clause: Only train models for specific datasets (e.g., Sentiment for Reviews)
    if ds_id != "reviews":
        print(f"Dataset '{ds_id}' does not require model training. Skipping.")
        return {
            "status": "skipped",
            "dataset": ds_id,
            "timestamp": datetime.datetime.now(ist_tz).isoformat()
        }

    # 2. Retrieve the file path from our Config Map
    # Ensure this points to the 'transformed' or 'prepared' file
    data_filepath = DATA_FILE_MAP.get(ds_id)

    if not data_filepath or not os.path.exists(str(data_filepath)):
        raise FileNotFoundError(f"Training failed: Required data file not found at {data_filepath}")

    # 3. Call the actual training logic
    # This function should handle MLflow logging internally
    try:
        results = train_and_evaluate(data_path=str(data_filepath), ds_id=ds_id)
        
        # Add IST metadata to the result before returning to XCom
        results["trained_at_ist"] = datetime.datetime.now(ist_tz).isoformat()
        
        print(f"Model training successfully completed for {ds_id}.")
        return results

    except Exception as e:
        print(f"Error during model training: {str(e)}")
        raise