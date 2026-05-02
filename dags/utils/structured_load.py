from airflow.providers.postgres.hooks.postgres import PostgresHook
import os
import logging
import pandas as pd

from dags.utils.config  import BASE_DATA_DIR, FEATURE_DIR

def load_structured_fn(ds_id, **kwargs):

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    
    hook = PostgresHook(postgres_conn_id='postgres_default')
    engine = hook.get_sqlalchemy_engine()
    conn = hook.get_conn()
    cur = conn.cursor()

    # feature_dir = os.path.join(FEATURE_DIR, 'features')
    feature_dir = FEATURE_DIR
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
