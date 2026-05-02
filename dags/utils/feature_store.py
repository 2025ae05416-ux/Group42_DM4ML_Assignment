import pandas as pd
from sqlalchemy import create_engine

class FeatureStore:
    def __init__(self, connection_string):
        self.engine = create_engine(connection_string)

    def get_features(self, feature_names, entity_id, version=None):
        """
        Retrieves features for a specific entity. 
        Version-based retrieval allows for A/B testing models.
        """
        # Logic to map feature_names to table schemas
        # If version is provided, we could pull from archival tables
        query = f"SELECT {','.join(feature_names)} FROM feature_store.interaction_features WHERE user_id = '{entity_id}'"
        return pd.read_sql(query, self.engine)

# Usage
fs = FeatureStore("postgresql://db_user:db_password@localhost:5432/db")
data = fs.get_features(['user_activity_freq', 'avg_order_value'], entity_id='USR123', version='v1_0')

# feature_store.py
def get_training_data(self, version="v1_0"):
    """Pulls all features to create a static training CSV/DF."""
    query = f"""
        SELECT i.*, s.similarity_score 
        FROM feature_store.fs_interaction_features_{version} i
        LEFT JOIN feature_store.fs_item_similarity_{version} s ON i.product_id = s.product_id
    """
    return pd.read_sql(query, self.engine)