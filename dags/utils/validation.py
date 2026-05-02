import pandas as pd
import pandera as pa
from pandera import Check, Column, DataFrameSchema

# --- Schemas: Structure Only (type coercion, no range/format checks) ---
# Range, format, and prefix checks are handled in the prepare step

users_schema = DataFrameSchema({
    "user_id":     Column(object, coerce=True, nullable=False),
    "email":       Column(object, coerce=True, nullable=True),
    "signup_date": Column(object, coerce=True, nullable=False),
    "gender":      Column(object, coerce=True, nullable=True),
}, strict=False)

reviews_schema = DataFrameSchema({
    "review_id":   Column(object, coerce=True, nullable=False),
    "rating":      Column(object, coerce=True, nullable=True),
    "review_date": Column(object, coerce=True, nullable=False),
}, strict=False)

products_schema = DataFrameSchema({
    "product_id":   Column(object, coerce=True, nullable=False),
    "price":        Column(object, coerce=True, nullable=True),
    "rating":       Column(object, coerce=True, nullable=True),
}, strict=False)

orders_schema = DataFrameSchema({
    "order_id":     Column(object, coerce=True, nullable=False),
    "order_date":   Column(object, coerce=True, nullable=False),
    "order_status": Column(object, coerce=True, nullable=True),
}, strict=False)

SCHEMAS = {
    "users":    users_schema,
    "reviews":  reviews_schema,
    "products": products_schema,
    "orders":   orders_schema,
}

def validate_data(df, ds_id):
    """
    Applies structural checks only (missing critical columns, duplicates).
    Range, format, and prefix validation is deferred to the prepare step.
    """
    stats = {
        'total_rows': len(df),
        'duplicates': int(df.duplicated().sum()),
        'missing_values': int(df.isnull().sum().sum())
    }

    errors = []

    # 1. Duplicate Check
    if stats['duplicates'] > 0:
        #errors.append(f"Integrity Error: Found {stats['duplicates']} duplicate rows.")
        print(f"[WARN] Found {stats['duplicates']} duplicate rows — will be handled in prepare step.")
    # 2. Critical Missing Values Only
    CRITICAL_COLUMNS = {
        "orders":   ["order_id", "order_date"],
        "reviews":  ["review_id", "review_date"],
        "users":    ["user_id", "signup_date"],
        "products": ["product_id"],
    }

    null_counts = df.isnull().sum()
    critical_cols = CRITICAL_COLUMNS.get(ds_id, [])
    for col, count in null_counts.items():
        if count > 0:
            if col in critical_cols:
                errors.append(f"Completeness Error: Column '{col}' has {count} missing values.")
            else:
                print(f"[WARN] Column '{col}' has {count} missing values — will be handled in prepare step.")

    # 3. Structural Check via Pandera (coerce only, no range/format)
    try:
        if ds_id in SCHEMAS:
            SCHEMAS[ds_id].validate(df, lazy=True)
    except pa.errors.SchemaErrors as err:
        for _, failure in err.failure_cases.iterrows():
            errors.append(f"Schema Error: Column '{failure['column']}' failed check '{failure['check']}' with value '{failure['failure_case']}'")

    return stats, errors