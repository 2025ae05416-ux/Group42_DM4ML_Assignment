import os
import random
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def generate_random_date(start_year=2025):
    start = datetime(start_year, 1, 1)
    end = datetime.now()
    delta = end - start
    days_limit = max(delta.days, 1)
    random_days = random.randrange(days_limit)
    random_seconds = random.randrange(86400)
    return (start + timedelta(days=random_days, seconds=random_seconds)).isoformat()

def inject_null(val, prob=0.08):
    """8% probability of returning NaN instead of the value."""
    return val if random.random() > prob else np.nan

def generate_all_data():
    from dags.utils.config import BASE_DATA_DIR
    # Update to your project path
    data_path = BASE_DATA_DIR
    os.makedirs(data_path, exist_ok=True)

    # MATCHING API RANGES: 
    # Users U000001-U010000 | Products P00001-P10000
    user_ids = [f"U{i:06d}" for i in range(1, 2001)]
    product_ids = [f"P{i:05d}" for i in range(1, 2001)]

    # 1. Generate 20,000 Orders
    orders = []
    for i in range(1, 20001):
        orders.append({
            "order_id": f"O{i:08d}",
            "user_id": inject_null(random.choice(user_ids)), # Nullable User
            "order_date": generate_random_date(),
            "order_status": random.choice(["completed", "pending", "cancelled"]),
            "total_amount": inject_null(round(random.uniform(10, 3000), 2)), # Nullable amount
        })
    
    orders_df = pd.DataFrame(orders)
    orders_df.to_csv(f"{data_path}/orders.csv", index=False)

    # 2. Generate Reviews (Scaling with orders)
    reviews = []
    review_pool = [
        (4, "Good product will buy again"),
        (1, "Not good wont buy at all"),
        (5, "Highly recommend this brand"),
        (2, "Color was different from images."),
        (2, "Not as expected, quality is poor."),
        (5, "Fast shipping and good packaging"),
        (4, "Value for money, satisfied with purchase."),
        (1, "Terrible quality, returned immediately."),
        (5, "Exceeded my expectations!"),
        (3, "Decent product but overpriced."),
    ]
    
    for _, order in orders_df.iterrows():
        # Only create a review for ~70% of orders to simulate real behavior
        if random.random() > 0.3: 
            rating_val, text_val = random.choice(review_pool)
            
            # Ensure we only use valid order IDs if the order_id isn't null
            if pd.notnull(order['order_id']):
                reviews.append({
                    "review_id": f"R{order['order_id'][1:]}",
                    "order_id": order['order_id'],
                    "product_id": random.choice(product_ids),
                    "user_id": order['user_id'],
                    "rating": inject_null(rating_val, prob=0.05), # 5% Null Ratings
                    "review_text": inject_null(text_val, prob=0.1), # 10% Null Text
                    "review_date": generate_random_date(),
                })

    reviews_df = pd.DataFrame(reviews)
    reviews_df.to_csv(f"{data_path}/reviews.csv", index=False)
    
    print(f"20,000 records generated.")
    print(f"Location: {data_path}")
    print(f"Null values injected for testing.")

if __name__ == "__main__":
    generate_all_data()