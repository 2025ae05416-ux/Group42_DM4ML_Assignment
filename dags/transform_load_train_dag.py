from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

from dags.task_utils import (
    transform_wrapper, 
    plot_wrapper, 
    feature_wrapper, 
    load_features_wrapper, 
    load_wrapper, 
    train_model_wrapper
)

with DAG(
    dag_id="transform_load_train_dag",
    start_date=datetime(2026, 4, 25),
    schedule=None,
    catchup=False,
) as dag:


    # -------------------------
    # TASKS
    # -------------------------
    # 1. Transform the cleaned data
    t1 = PythonOperator(task_id="transform_dataset", python_callable=transform_wrapper)

    # 2. Visual Validation (Generates the PDF report and plots)
    t2 = PythonOperator(task_id="generate_plots", python_callable=plot_wrapper)

    # 3. Feature Engineering (Nulls/Range errors)
    t3 = PythonOperator(task_id="engineer_features", python_callable=feature_wrapper)

    # 4. Feature Store Loading
    t4 = PythonOperator(task_id="load_features", python_callable=load_features_wrapper)

    # 6. Dataset Loading to PostgreSQL
    t6 = PythonOperator(task_id="load_dataset", python_callable=load_wrapper)

    # 5. Model Training (Uses MLflow)
    t5 = PythonOperator(task_id="train_model_task", python_callable=train_model_wrapper)

    # Pipeline
    # t1 >> t2 >> t3 >> t4 >> t5 >> t6
    #t1 >> t3 >> t4 >> t5 >> t6
    #t1 >> t2
    #t1 >> t6
    t1 >> t3 
    [t1, t3] >> t2  # t2 starts only after both t1 and t3 are done
    t2 >> t4 >> t5 >> t6
