import logging

import joblib
from matplotlib import table
from matplotlib.pyplot import table
import pandas as pd
import matplotlib
from pandera.validation_depth import logger
matplotlib.use('Agg')  # Essential for headless Airflow workers
import numpy as np
from datetime import datetime
import pandera.errors as pa_errors

DATA_PATH = '/opt/airflow/data'
DATASET_CONFIG = [
    {"id": "products", "kaggle_slug": "abhayayare/e-commerce-dataset", "file_name": "products.csv", "target_schema": "dmml_assignment", "target_table": "products", "pk": "product_id"},
    {"id": "users", "kaggle_slug": "abhayayare/e-commerce-dataset", "file_name": "users.csv", "target_schema": "dmml_assignment", "target_table": "users", "pk": "user_id"},
    {"id": "orders", "file_name": "orders.csv", "target_schema": "dmml_assignment", "target_table": "orders", "pk": "order_id"},
    {"id": "reviews", "file_name": "reviews.csv", "target_schema": "dmml_assignment", "target_table": "reviews", "pk": "review_id"}
]
    # Mapping of ds_id to their required columns for specific tables
DB_SCHEMA_MAP = {
    'orders': {
        'interaction_features': ['user_id', 'product_id', 'rating', 'user_activity_freq', 'user_avg_rating', 'item_avg_rating'],
        'reviews': None # No review data here
    },
    'reviews': {
        'interaction_features': ['user_id', 'product_id', 'rating', 'user_activity_freq', 'user_avg_rating', 'item_avg_rating'],
        'user_reviews': ['user_id', 'product_id', 'review_text', 'created_at']
    }
}


def extract_fn(kaggle_slug, file_name, **kwargs):
    import os
    import shutil
    from kaggle.api.kaggle_api_extended import KaggleApi

    # 1. Setup specific path for the target file
    target_file_path = os.path.join(DATA_PATH, file_name)

    # Idempotency check: Skip if the specific file already exists
    if os.path.exists(target_file_path):
        print(f"DEBUG: {file_name} already exists. Skipping download.")
        return

    # 2. Setup Temporary Extraction Directory
    ti = kwargs['ti']
    temp_dir = f"/tmp/kaggle_extract_{ti.dag_id}_{ti.task_id}_{ti.try_number}"
    os.makedirs(temp_dir, exist_ok=True)
    
    # 3. Handle Kaggle Authentication
    # Ensure your credentials exist where the API expects them
    # If your kaggle.json is in the default home location, use that.
    # Otherwise, copy it to the temp_dir:
    # shutil.copy('/home/airflow/.kaggle/kaggle.json', os.path.join(temp_dir, 'kaggle.json'))
    
    os.environ['KAGGLE_CONFIG_DIR'] = '/home/airflow/.kaggle' 
    
    # 4. Authenticate and Download
    api = KaggleApi()
    api.authenticate()
    
    print(f"DEBUG: Downloading {kaggle_slug}...")
    api.dataset_download_files(kaggle_slug, path=temp_dir, unzip=True)
    
    # 5. Filter and Move the file
    found_files = []
    for root, dirs, files in os.walk(temp_dir):
        if file_name in files:
            found_files.append(os.path.join(root, file_name))
            
    if not found_files:
        raise FileNotFoundError(f"Could not find {file_name} in downloaded files.")

    shutil.move(found_files[0], target_file_path)
    print(f"DEBUG: Successfully moved {file_name} to {DATA_PATH}")

    # 6. Cleanup temporary directory
    shutil.rmtree(temp_dir)
    print("DEBUG: Cleaned up temporary files.")

def transform_fn(file_name, ds_id, **kwargs):
    import pandas as pd
    import os
    import sys
    import pandera as pa
    from datetime import datetime
    from collections import Counter

    # 1. Path Setup
    dags_path = '/opt/airflow/dags'
    if dags_path not in sys.path: sys.path.append(dags_path)

    try:
        from validation import SCHEMAS
        from reporting import generate_pdf_quality_report
    except ImportError:
        SCHEMAS = {}
        def generate_pdf_quality_report(*args, **kwargs): pass

    file_path = os.path.join(DATA_PATH, file_name)
    df = pd.read_csv(file_path)
    df.columns = [c.lower().replace(' ', '_') for c in df.columns]

    # 2. RUN PREPROCESSING 
    df = preprocess_fn(df)

    # 3. Schema Alignment (Prevents COLUMN_NOT_IN_SCHEMA)
    schema = SCHEMAS.get(ds_id)

    if schema:
        # Standardize known common mismatches
        df = df.rename(columns={'order_status': 'status', 'review_text': 'comment'})
        # Keep only columns defined in the schema
        expected_cols = list(schema.columns.keys())
        df = df[[c for c in df.columns if c in expected_cols]]

    # 4. Robust Date Parsing
    for col in ['signup_date', 'review_date', 'order_date']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce', format='mixed')

    # 5. Validation
    stats = {
        'total_rows': len(df),
        'duplicates': int(df.duplicated().sum()),
        'missing_values': int(df.isnull().sum().sum())
    }
    
    errors = []
    validated_df = df
    if schema:
        try:
            validated_df = schema.validate(df, lazy=True)
            print(f"DEBUG: Columns surviving schema validation for {ds_id}: {validated_df.columns.tolist()}")
        except pa_errors.SchemaErrors as err:
            # 1. Use a list to store only the error descriptions (not the row index)
            for _, failure in err.failure_cases.iterrows():
                # Clean error description: handle 'None' column names
                col = failure.get('column') or "Schema/Structure"
                msg = failure.get('failure_case') or "Validation failure"
                errors.append(f"Column '{col}': {msg}")

    # 6. Always Generate Report (Success or Failure)
    report_dir = os.path.join(DATA_PATH, 'reports')
    os.makedirs(report_dir, exist_ok=True)
    report_path = os.path.join(report_dir, f"{ds_id}_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf")
    
    generate_pdf_quality_report(ds_id, stats, errors, report_path)
    
    # 7. Final Task Outcome
    if errors:
        print(f"WARNING: Quality validation issues found. Check report: {report_path}")

    validated_df.to_csv(os.path.join(DATA_PATH, f"transformed_{ds_id}.csv"), index=False)

def load_fn(target_schema, target_table, pk, ds_id, **kwargs):
    import pandas as pd
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    df = pd.read_csv(f"{DATA_PATH}/transformed_{ds_id}.csv")
    
    # Generate the column names and update/insert clauses dynamically
    columns = df.columns.tolist()
    # Create string: "col1 = EXCLUDED.col1, col2 = EXCLUDED.col2..."
    update_stmt = ", ".join([f"{col} = EXCLUDED.{col}" for col in columns if col != pk])
    
    # Prepare the INSERT query
    sql = f"""
        INSERT INTO {target_schema}.{target_table} ({", ".join(columns)})
        VALUES ({", ".join(["%s"] * len(columns))})
        ON CONFLICT ({pk}) 
        DO UPDATE SET {update_stmt};
    """
    
    # Execute for every row
    rows = [tuple(x) for x in df.to_numpy()]
    postgres_hook.insert_rows(
        table=f"{target_schema}.{target_table}",
        rows=rows,
        target_fields=columns,
        replace=True, # Note: PostgresHook's built-in replace is basic; 
                      # consider using run() with the custom SQL above for full control.
    )

def archive_file(file_name, ds_id):
    import os
    import shutil
    from datetime import datetime
    
    # Generate timestamp components for partitioning
    now = datetime.now()
    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")
    
    # Define paths based on source and date partitions
    # Layout: data/archive/type/source/year/month/day/file
    raw_dest_dir = os.path.join(DATA_PATH, 'archive', 'raw', ds_id, year, month, day)
    trans_dest_dir = os.path.join(DATA_PATH, 'archive', 'transformed', ds_id, year, month, day)
    
    os.makedirs(raw_dest_dir, exist_ok=True)
    os.makedirs(trans_dest_dir, exist_ok=True)
    
    raw_source = os.path.join(DATA_PATH, file_name)
    transformed_source = os.path.join(DATA_PATH, f"transformed_{ds_id}.csv")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Helper to move and delete
    def move_to_archive(source, destination_dir):
        # ADDED: This check prevents the FileNotFoundError
        if os.path.exists(source):
            os.makedirs(destination_dir, exist_ok=True)
            filename = os.path.basename(source)
            dest_path = os.path.join(destination_dir, filename)
            shutil.move(source, dest_path)
            print(f"DEBUG: Archived {source} to {dest_path}")
        else:
            print(f"DEBUG: File not found, skipping archive for: {source}")

    move_to_archive(raw_source, raw_dest_dir)
    move_to_archive(transformed_source, trans_dest_dir)

def preprocess_fn(df):

    from sklearn.preprocessing import StandardScaler, LabelEncoder
    """
    Cleans and preprocesses data. Handles missing values, encodes categories,
    and normalizes numerical features only if columns exist in the DataFrame.
    """
    # Normalize all column names to lower case and replace spaces/hyphens
    df.columns = [c.lower().strip().replace(' ', '_').replace('-', '_') for c in df.columns]

    # 1. Handle Missing Interactions (Defensive)
    # Only drop rows if both 'user_id' and 'product_id' exist
    if 'user_id' in df.columns and 'product_id' in df.columns:
        df.dropna(subset=['user_id', 'product_id'], inplace=True)
    else:
        print(f"DEBUG: Skipping user/product dropna. Found columns: {df.columns.tolist()}")

    # Fill price if it exists
    if 'price' in df.columns:
        df['price'] = df['price'].fillna(df['price'].median())
    
    # 2. Encode Categorical Attributes
    if 'category' in df.columns:
        le = LabelEncoder()
        df['category_encoded'] = le.fit_transform(df['category'].astype(str))
        
    # 3. Normalize Numerical Variables
    if 'price' in df.columns:
        scaler = StandardScaler()
        # Ensure we are passing a 2D array for the scaler
        df['price_scaled'] = scaler.fit_transform(df[['price']])
    
    return df

def plot_fn(ds_id, **kwargs):
    """
    Reads the transformed data for a given ds_id and generates a visualization.
    Saves the plot to the reports directory for easy access.
    """
    import pandas as pd
    import matplotlib.pyplot as plt
    import seaborn as sns
    import os
    
    file_path = os.path.join(DATA_PATH, f"transformed_{ds_id}.csv")
    
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Transformed file not found at {file_path}")

    df = pd.read_csv(file_path)
    
    # Define a report directory for plots
    report_dir = os.path.join(DATA_PATH, 'reports', 'plots')
    os.makedirs(report_dir, exist_ok=True)
    
    # Example logic: Plotting based on dataset type
    plt.figure(figsize=(10, 6))
    
    if ds_id == 'users' and 'gender' in df.columns:
        sns.countplot(data=df, x='gender')
    elif ds_id == 'products' and 'price' in df.columns:
        sns.histplot(data=df, x='price', bins=30)
    elif ds_id == 'orders' and 'rating' in df.columns:
        # New custom logic for orders
        sns.histplot(data=df, x='rating', discrete=True)
        plt.title(f"Order Rating Distribution - {ds_id}")
    else:
        # Keep your fallback for unknown datasets
        df.head(10).plot(kind='bar', subplots=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filename = f"{ds_id}_plot_{timestamp}.png"

    output_path = os.path.join(report_dir, output_filename)
    plt.savefig(output_path)
    plt.close()
    
    print(f"DEBUG: Successfully generated and saved plot to {output_path}")

# Feature Engineering and Transformation Functions for Recommendations

def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    required = ['user_id', 'product_id', 'rating']
    missing = [c for c in required if c not in df.columns]
    
    if missing:
        print(f"DEBUG: Missing columns: {missing}")       
        #raise ValueError(f'Missing columns {missing}')

    # Calculate metrics
    user_counts = df.groupby('user_id')['product_id'].count().rename('user_activity_freq')
    user_avg = df.groupby('user_id')['rating'].mean().rename('user_avg_rating')
    item_avg = df.groupby('product_id')['rating'].mean().rename('item_avg_rating')

    # Merge features
    df = df.merge(user_counts, on='user_id') \
           .merge(user_avg, on='user_id') \
           .merge(item_avg, on='product_id')
    
    return df

def generate_recommendation_features(df):

    import pandas as pd
    import numpy as np
    from sklearn.metrics.pairwise import cosine_similarity
    """
    Computes Co-occurrence and Cosine Similarity features
    to be used by recommendation algorithms.
    """
    # 1. Pivot to User-Item Matrix
    pivot = pd.crosstab(df['user_id'], df['product_id'])
    
    # 2. Co-occurrence Matrix
    cooccurrence = pivot.T.dot(pivot)
    np.fill_diagonal(cooccurrence.values, 0)
    
    # 3. Similarity Matrix (Cosine)
    similarity_matrix = cosine_similarity(cooccurrence)
    sim_df = pd.DataFrame(similarity_matrix, 
                          index=cooccurrence.index, 
                          columns=cooccurrence.columns)
    
    return sim_df

def get_top_n_similar_items(similarity_df, n=3):
    import pandas as pd
    """
    Converts matrix to a structured format for database storage.
    """
    top_n_list = []
    for product in similarity_df.columns:
        similar = similarity_df[product].nlargest(n+1).index.tolist()
        # Remove the product itself from its own "similar" list
        if product in similar:
            similar.remove(product)
        top_n_list.append({'product_id': product, 'similar_product_id': similar[0], 'similarity_score': similarity_df.loc[similar[0], product], 'rank_position': 1})
    
    return pd.DataFrame(top_n_list)

def get_subset_df(df, ds_id, target_table):
    """Checks for required columns and returns a subset DataFrame safely."""
    mapping = DB_SCHEMA_MAP.get(ds_id, {})
    required_cols = mapping.get(target_table)
    
    if not required_cols:
        return None
    
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        print(f"DEBUG: Skipping load for {target_table}. Missing columns: {missing}")
        return None
        
    return df[required_cols]

def feature_engineering_fn(ds_id):
    import pandas as pd
    import os
    import logging # 1. Import logging
    logger = logging.getLogger("airflow.task")

    input_path = f"{DATA_PATH}/transformed_{ds_id}.csv"
    feature_dir = os.path.join(DATA_PATH, 'features')
    os.makedirs(feature_dir, exist_ok=True)

    df = pd.read_csv(input_path)


    # --------------------------------------------------
    # REVIEWS: Recommendation Features
    # --------------------------------------------------
    if ds_id == 'reviews':

        # 1. Interaction Features
        interaction_df = engineer_features(df)

        interaction_df.to_csv(
            os.path.join(
                feature_dir,
                f"interaction_features_{ds_id}.csv"
            ),
            index=False
        )
        logger.info(f"Successfully saved interaction_features to: {feature_dir}/interaction_features_{ds_id}.csv")

        # 2. Similarity Features
        sim_matrix = generate_recommendation_features(
            interaction_df
        )

        sim_df = get_top_n_similar_items(
            sim_matrix,
            n=5
        )

        sim_df.to_csv(
            os.path.join(
                feature_dir,
                f"item_similarity_{ds_id}.csv"
            ),
            index=False
        )
        logger.info(f"Successfully saved item_similarity to: {feature_dir}/item_similarity_{ds_id}.csv")

    # --------------------------------------------------
    # ORDERS: Behavioral Features
    # --------------------------------------------------
    elif ds_id == 'orders':

        required_cols = [
            'user_id',
            'order_date',
            'total_amount'
        ]

        missing = [
            c for c in required_cols
            if c not in df.columns
        ]

        if missing:
            raise ValueError(
                f"Missing columns {missing}"
            )

        df['order_date'] = pd.to_datetime(
            df['order_date']
        )

        today = pd.Timestamp.today()


        # Order Frequency
        order_freq = (
            df.groupby('user_id')['order_id']
            .count()
            .rename('order_frequency')
        )


        # Average Spend
        avg_spend = (
            df.groupby('user_id')['total_amount']
            .mean()
            .rename('avg_order_value')
        )


        # Total Spend
        total_spend = (
            df.groupby('user_id')['total_amount']
            .sum()
            .rename('total_spend')
        )


        # Recency
        last_order = (
            df.groupby('user_id')['order_date']
            .max()
        )

        recency = (
            (today - last_order)
            .dt.days
            .rename('days_since_last_order')
        )


        # Avg Days Between Orders
        df = df.sort_values(
            ['user_id','order_date']
        )

        df['days_between_orders'] = (
            df.groupby('user_id')['order_date']
            .diff()
            .dt.days
        )

        avg_gap = (
            df.groupby('user_id')['days_between_orders']
            .mean()
            .fillna(0)
            .rename('avg_days_between_orders')
        )


        # Combine features
        orders_features = pd.concat(
            [
                order_freq,
                avg_spend,
                total_spend,
                recency,
                avg_gap
            ],
            axis=1
        ).reset_index()


        orders_features.to_csv(
            os.path.join(
                feature_dir,
                "features_orders.csv"
            ),
            index=False
        )


    # --------------------------------------------------
    # USERS
    # --------------------------------------------------
    elif ds_id == 'users':

        if 'signup_date' not in df.columns:
            raise KeyError(
                f"Column signup_date missing"
            )

        df['days_since_signup'] = (
            pd.to_datetime('today')
            - pd.to_datetime(df['signup_date'])
        ).dt.days

        df[
            ['user_id','days_since_signup']
        ].to_csv(
            os.path.join(
                feature_dir,
                "features_users.csv"
            ),
            index=False
        )


    # --------------------------------------------------
    # PRODUCTS
    # --------------------------------------------------
    elif ds_id == 'products':

        if 'category' not in df.columns:
            raise KeyError(
                f"Column category missing"
            )

        df['category_code'] = (
            df['category']
            .astype('category')
            .cat.codes
        )

        df[
            [
                'product_id',
                'category_code',
                'price'
            ]
        ].to_csv(
            os.path.join(
                feature_dir,
                "features_products.csv"
            ),
            index=False
        )


    else:
        raise ValueError(
            f"CRITICAL: No feature engineering logic defined for dataset: {ds_id}"
        )
        

def load_structured_fn(ds_id, **kwargs):
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    import os
    import logging
    import pandas as pd

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    
    hook = PostgresHook(postgres_conn_id='postgres_default')
    engine = hook.get_sqlalchemy_engine()
    conn = hook.get_conn()
    cur = conn.cursor()

    feature_dir = os.path.join(DATA_PATH, 'features')
    # Use the new Feature Store schema and version
    VERSION = "v1_0"
    TARGET_SCHEMA = "feature_store" 

    tables = {
        'interaction_features': f"{feature_dir}/interaction_features_{ds_id}.csv",
        'item_similarity': f"{feature_dir}/item_similarity_{ds_id}.csv"
    }

    for table, file in tables.items():
        if not os.path.exists(file):
            logger.warning(f"Feature file missing: {file}. Skipping load for {table}.")
            continue
            
        df = pd.read_csv(file)
        
        # 1. Apply Filtering & Deduplication
        if table == 'interaction_features':
            allowed_cols = ['user_id', 'product_id', 'rating', 'user_activity_freq', 'user_avg_rating', 'item_avg_rating']
            df = df[[c for c in allowed_cols if c in df.columns]]
            df = df.drop_duplicates(subset=['user_id', 'product_id'], keep='last')
            conflict = '(user_id, product_id)'
        elif table == 'item_similarity':
            allowed_cols = ['product_id', 'similar_product_id', 'similarity_score', 'rank_position']
            df = df[[c for c in allowed_cols if c in df.columns]]
            df = df.drop_duplicates(subset=['product_id', 'similar_product_id'], keep='last')
            conflict = '(product_id, similar_product_id)'
        
        # 2. Target Versioned Table Name
        versioned_table_name = f"fs_{table}_{VERSION}"
        stage_table = f'{TARGET_SCHEMA}.{table}_stage'
        target_full_name = f'{TARGET_SCHEMA}.{versioned_table_name}'
        
        # 3. Load to staging
        df.to_sql(f'{table}_stage', engine, schema=TARGET_SCHEMA, if_exists='replace', index=False)
        
        cols = df.columns.tolist()
        update_clause = ', '.join([f'{c}=EXCLUDED.{c}' for c in cols if c not in ['product_id', 'similar_product_id', 'user_id']])
        
        # 4. Transactional Upsert
        sql = f'''
            INSERT INTO {target_full_name} ({','.join(cols)})
            SELECT {','.join(cols)} FROM {stage_table}
            ON CONFLICT {conflict}
            DO UPDATE SET {update_clause};
        '''
        
        try:
            cur.execute('BEGIN;')
            cur.execute(sql)
            cur.execute('COMMIT;')
            logger.info(f'Merged {len(df)} rows into {target_full_name} successfully.')
        except Exception as e:
            cur.execute('ROLLBACK;')
            logger.error(f"Failed to load {table}: {e}")
            raise e
            
    conn.close()

