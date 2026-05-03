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
    return val if random.random() > prob else np.nan

def generate_all_data():
    from dags.utils.config import BASE_DATA_DIR
    data_path = BASE_DATA_DIR
    os.makedirs(data_path, exist_ok=True)

    # These ranges MUST match your API's TOTAL_RECORDS
    user_ids = [f"U{i:06d}" for i in range(1, 2001)]
    product_ids = [f"P{i:05d}" for i in range(1, 2001)]

    # --- 1. Generate 2000 Orders ---
    orders = []
    for i in range(1, 2001):
        uid = random.choice(user_ids)
        
        # CONDITIONAL LOGIC: Premium User Simulation
        # If user_id index is divisible by 5, they are "High Spenders"
        u_idx = int(uid[1:])
        if u_idx % 5 == 0:
            total_amount = round(random.uniform(500, 3000), 2)
            status = random.choice(["completed", "completed", "pending"]) # More likely to complete
        else:
            total_amount = round(random.uniform(10, 500), 2)
            status = random.choice(["completed", "cancelled", "pending"])

        orders.append({
            "order_id": f"O{i:08d}",
            "user_id": inject_null(uid),
            "order_date": generate_random_date(),
            "order_status": status,
            "total_amount": inject_null(total_amount),
        })
    
    orders_df = pd.DataFrame(orders)
    orders_df.to_csv(f"{data_path}/orders.csv", index=False)

    # --- 2. Generate Reviews (The ML Target Signal) ---
    reviews = []
    
    # We define specific behaviors linked to the User ID
    # Even User IDs = Happy customers (Ratings 4-5)
    # Odd User IDs = Critical customers (Ratings 1-3)
    
    positive_pool = [(5, "Exceeded my expectations!"), (4, "Good product will buy again"), (5, "Highly recommend")]
    negative_pool = [(1, "Terrible quality"), (2, "Not as expected"), (3, "Overpriced")]

    for _, order in orders_df.iterrows():
        # Only ~70% leave reviews
        if random.random() > 0.3 and pd.notnull(order['user_id']):
            uid = order['user_id']
            u_idx = int(uid[1:])
            
            # CONDITIONAL SIGNAL: The model will learn that User ID parity correlates to rating
            if u_idx % 2 == 0:
                rating_val, text_val = random.choice(positive_pool)
            else:
                rating_val, text_val = random.choice(negative_pool)
            
            # Add some "noise" so it's not 100% predictable (realistic ML)
            if random.random() > 0.9:
                rating_val = random.randint(1, 5)

            reviews.append({
                "review_id": f"R{order['order_id'][1:]}",
                "order_id": order['order_id'],
                "product_id": random.choice(product_ids),
                "user_id": uid,
                "rating": inject_null(rating_val, prob=0.05),
                "review_text": inject_null(text_val, prob=0.1),
                "review_date": generate_random_date(),
            })

    reviews_df = pd.DataFrame(reviews)
    reviews_df.to_csv(f"{data_path}/reviews.csv", index=False)
    
    print(f"Relational datasets generated with ML-ready signals at: {data_path}")

if __name__ == "__main__":
    generate_all_data()