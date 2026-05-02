from pathlib import Path

DATASET_CONFIG = [
    {"id": "products","file_name": "products.csv", "target_schema": "dmml_assignment", "target_table": "products", "pk": "product_id"},
    {"id": "users", "file_name": "users.csv", "target_schema": "dmml_assignment", "target_table": "users", "pk": "user_id"},
    {"id": "orders", "file_name": "orders.csv", "target_schema": "dmml_assignment", "target_table": "orders", "pk": "order_id"},
    {"id": "reviews", "file_name": "reviews.csv", "target_schema": "dmml_assignment", "target_table": "reviews", "pk": "review_id"}
]

DB_SCHEMA_MAP = {
    "orders": {
        "interaction_features": [
            "user_id","product_id","rating",
            "user_activity_freq",
            "user_avg_rating",
            "item_avg_rating"
        ],
        "reviews": None
    }
}
# Define base paths
BASE_DATA_DIR = Path("/opt/airflow/data")
FEATURE_DIR = BASE_DATA_DIR / "features"

# Create a map of ready-to-use absolute paths
DATA_FILE_MAP = {
    "reviews": FEATURE_DIR / "interaction_features_reviews.csv",
    "users": BASE_DATA_DIR / "transformed_users.csv",
    "products": BASE_DATA_DIR / "transformed_products.csv",
    "orders": BASE_DATA_DIR / "transformed_orders.csv",
}