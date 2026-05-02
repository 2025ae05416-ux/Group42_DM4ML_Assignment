import numpy as np
import pandas as pd

def prepare_data_fn(df, ds_id):
    import mlflow
    """
    Applies fixes for missing values, duplicates, and range/format issues.
    Ensures compatibility with Postgres by converting NaNs to None.
    """
    span = mlflow.get_current_active_span()
    # 1. Handle Duplicates
    initial_count = len(df)
    df = df.drop_duplicates().reset_index(drop=True)
    if len(df) < initial_count:
        print(f"Removed {initial_count - len(df)} duplicate rows.")
    
    if span:
        span.set_attributes({"initial_rows": initial_count})
    

    # 2. Dataset-Specific Cleaning
    if ds_id == "reviews":
        # CRITICAL: Fill missing ratings BEFORE clipping
        # Using 0 or a median helps avoid NULLs in your recommendation matrix
        df["rating"] = pd.to_numeric(df["rating"], errors='coerce').fillna(0)
        df["review_text"] = df["review_text"].fillna("")
        df["rating"] = df["rating"].clip(1, 5)

    elif ds_id == "products":
        df["category"] = df["category"].fillna("Unknown")
        df["brand"] = df["brand"].fillna("Unknown")
        df["price"] = pd.to_numeric(df["price"], errors='coerce').fillna(0).clip(lower=0)
        df["rating"] = pd.to_numeric(df["rating"], errors='coerce').fillna(0).clip(0, 5)

    elif ds_id == "users":
        df["name"] = df["name"].fillna("Unknown User")
        df["city"] = df["city"].fillna("Unknown City")
        before = len(df)
        df = df.dropna(subset=["email"])
        if len(df) < before:
            print(f"[PREPARE] Dropped {before - len(df)} users with missing email.")

        valid_genders = ["Male", "Female", "Other", "Non-binary"]
        df["gender"] = df["gender"].apply(lambda x: x if x in valid_genders else "Other")

    elif ds_id == "orders":
        df["total_amount"] = pd.to_numeric(df["total_amount"], errors='coerce').fillna(0).clip(lower=0)
        df["order_status"] = df["order_status"].fillna("Unknown")

    # 3. Final Schema Guard
    critical_cols = {
        "users": "user_id", "orders": "order_id", 
        "reviews": "review_id", "products": "product_id"
    }
    
    id_col = critical_cols.get(ds_id)
    if id_col and id_col in df.columns:
        df = df.dropna(subset=[id_col])

    # --- THE FIX FOR YOUR LOG ERROR ---
    # Convert any remaining NaN/NaT to None. 
    # This turns them into SQL NULLs instead of 'nan' strings that crash Postgres.
    df = df.replace({np.nan: None, pd.NA: None, pd.NaT: None})
    
    
    if span:
        span.set_attributes({"final_rows": len(df)})
    return df