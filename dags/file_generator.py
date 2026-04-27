import pandas as pd
import random
import os
import random
from datetime import datetime, timedelta
#from faker import Faker

def generate_all_data():
    """Logic to generate and save CSV files."""
    #fake = Faker()
    data_path = "/opt/airflow/data"
    os.makedirs(data_path, exist_ok=True)
    
    user_ids = [f"U{i:05d}" for i in range(1000, 1100)]
    product_ids = [f"P{i:06d}" for i in range(1, 101)]
    
    # Generate Orders
    orders = [{"order_id": f"O{i:08d}", "user_id": random.choice(user_ids), 
               "order_date": generate_random_date(),
               "order_status": random.choice(['completed', 'pending']), 
               "total_amount": round(random.uniform(10, 3000), 2)} for i in range(1, 21)]
    pd.DataFrame(orders).to_csv(f"{data_path}/orders.csv", index=False)
    
    # Generate Reviews
    reviews = [{"review_id": f"R{i:08d}", "order_id": f"O{i:08d}", "product_id": random.choice(product_ids),
                "user_id": random.choice(user_ids), "rating": random.randint(1, 5),
                "review_text": "Good product", "review_date": generate_random_date()} 
               for i in range(1, 21)]
    pd.DataFrame(reviews).to_csv(f"{data_path}/reviews.csv", index=False)
    
    print("Files generated in", data_path)


# Helper to generate random date
def generate_random_date(start_year=2025):
    start = datetime(start_year, 1, 1)
    end = datetime.now()
    delta = end - start
    random_days = random.randrange(delta.days + 1)
    random_seconds = random.randrange(86400) # 86400 seconds in a day
    return (start + timedelta(days=random_days, seconds=random_seconds)).isoformat()
