import pandas as pd
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity

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

def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    import pandas as pd

    # Downcast for memory efficiency immediately
    if 'rating' in df.columns:
        df['rating'] = df['rating'].astype('float32')

    required = ['user_id', 'product_id', 'rating']
    missing = [c for c in required if c not in df.columns]
    
    if missing:
        print(f"DEBUG: Missing columns: {missing}")
        return df # Return early to avoid crashing on downstream logic

    # Use Series Mapping instead of Merges
    print("DEBUG: Calculating user and item metrics...")
    
    # Calculate stats as Series (dictionaries)
    user_counts = df.groupby('user_id')['product_id'].transform('count').astype('int32')
    user_avg = df.groupby('user_id')['rating'].transform('mean').astype('float32')
    item_avg = df.groupby('product_id')['rating'].transform('mean').astype('float32')

    # Assign directly to the existing dataframe (In-place operation)
    df['user_activity_freq'] = user_counts
    df['user_avg_rating'] = user_avg
    df['item_avg_rating'] = item_avg
    
    print(f"DEBUG: Feature engineering complete. Rows: {len(df)}")
    return df

def generate_recommendation_features(df):


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
