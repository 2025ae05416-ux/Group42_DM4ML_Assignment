import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook


DB_COLUMNS = {
    "products": [
        "product_id",
        "product_name",
        "category",
        "brand",
        "price",
        "rating",
        "stock_quantity"
    ],
    "users": [
        "user_id",
        "name",
        "email",
        "gender",
        "city",
        "signup_date"
    ],
    "orders": [
        "order_id",
        "user_id",
        "product_id",
        "order_date",
        "total_amount",
        "order_status"
    ],
    "reviews": [
        "review_id",
        "user_id",
        "order_id",
        "product_id",
        "rating",
        "review_text",
        "review_date"
    ]
}
def load_fn(target_schema, target_table, pk, ds_id, **kwargs):
    from dags.utils.config import BASE_DATA_DIR

    hook = PostgresHook(
        postgres_conn_id="postgres_default"
    )

    df = pd.read_csv(
        f"{BASE_DATA_DIR}/transformed_{ds_id}.csv"
    )

    print("Before filtering:", df.columns.tolist())

    allowed_cols = DB_COLUMNS[ds_id]

    df = df[
        [c for c in allowed_cols if c in df.columns]
    ]

    print("After filtering:", df.columns.tolist())

    rows = [tuple(x) for x in df.to_numpy()]

    hook.insert_rows(
        table=f"{target_schema}.{target_table}",
        rows=rows,
        target_fields=df.columns.tolist(),
        replace=True
    )