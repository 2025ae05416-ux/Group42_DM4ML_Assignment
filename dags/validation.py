import pandas as pd
import pandera as pa
from pandera import Check, Column, DataFrameSchema
from datetime import datetime
import os
#from weasyprint import HTML

# Schema for Users
users_schema = DataFrameSchema({
    "user_id": Column(str, Check.str_startswith("U")),
    "email": Column(str, Check.str_matches(r"[^@]+@[^@]+\.[^@]+")),
    "signup_date": Column(pa.DateTime),
    "name": pa.Column(pa.String, nullable=True),
    "gender": pa.Column(pa.String, nullable=True),
    "city": pa.Column(pa.String, nullable=True),
}, strict=False)

# Schema for Orders
orders_schema = DataFrameSchema({
    "order_id": Column(str, Check.str_startswith("O")),
    "user_id": Column(str, Check.str_startswith("U")),
    "total_amount": Column(float, Check.ge(0)),
    "order_status": Column(str),
    "order_date": pa.Column(pa.DateTime, nullable=True),
}, strict=False)

# Schema for Reviews
reviews_schema = DataFrameSchema({
    "review_id": Column(str, Check.str_startswith("R")),
    "product_id": Column(str, Check.str_startswith("P")),
    "rating": Column(int, Check.in_range(1, 5)), # Rating scale 1-5
    "review_text": Column(str, nullable=True),
    "review_date": pa.Column(pa.DateTime),
    "order_id": Column(str, Check.str_startswith("O")),
    "user_id": Column(str, Check.str_startswith("U")),
}, strict=False)

# Schema for Products
products_schema = DataFrameSchema({
    "product_id": Column(str, Check.str_startswith("P")),
    "product_name": Column(str, nullable=True),
    "price": Column(float, Check.ge(0)),
    "category": Column(str, nullable=True),
    "brand": Column(str, nullable=True),
    "rating": Column(float, Check.in_range(0, 5)), # Average rating scale 0-5
}, strict=False)

# Map for easy access in your pipeline
SCHEMAS = {
    "users": users_schema,
    "orders": orders_schema,
    "reviews": reviews_schema,
    "products": products_schema
}
def validate_data(df, ds_id):
    stats = {
        'total_rows': len(df),
        'duplicates': df.duplicated().sum(),
        'missing_values': df.isnull().sum().sum()
    }
    
    errors = []
    try:
        SCHEMAS[ds_id].validate(df, lazy=True)
    except pa.errors.SchemaErrors as err:
        # Capture specific validation failures
        for _, failure in err.failure_cases.iterrows():
            errors.append(f"Column '{failure['column']}': {failure['failure_case']} at row {failure['index']}")
            
    return stats, errors