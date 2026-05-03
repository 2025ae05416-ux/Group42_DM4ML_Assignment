from fastapi import FastAPI
import random
import numpy as np
from faker import Faker
from typing import Optional

app = FastAPI()
fake = Faker()

# Configuration
TOTAL_RECORDS = 2000
NULL_PROBABILITY = 0.05  # Reduced slightly to ensure enough signal for ML

def inject_null(value):
    return value if random.random() > NULL_PROBABILITY else None

# --- USERS GENERATOR ---
def generate_users(n=TOTAL_RECORDS):
    data = []
    for i in range(1, n + 1):
        # STRATEGY: Age and Loyalty Score will be our "Predictors"
        age = random.randint(18, 70)
        # Conditional Logic: Older users tend to have higher loyalty scores in this data
        loyalty_base = 0.6 if age > 45 else 0.3
        loyalty_score = min(1.0, loyalty_base + random.uniform(0, 0.4))

        data.append({
            "user_id": f"U{i:06d}",
            "name": fake.name(),
            "age": age,
            "email": f"user{i:06d}@example.com",
            "city": inject_null(fake.city()),
            "loyalty_score": round(loyalty_score, 2),
            "signup_date": fake.date_between(start_date='-2y', end_date='today').strftime('%Y-%m-%d')
        })
    return data

@app.get("/users")
async def get_users(count: int = TOTAL_RECORDS):
    return generate_users(count)

# --- PRODUCTS GENERATOR ---
def generate_products(n=TOTAL_RECORDS):
    category_brand_map = {
        "Electronics": {"brands": ["Apple", "Samsung", "Sony"], "base_price": 400},
        "Home & Garden": {"brands": ["Ikea", "Dyson", "Philips"], "base_price": 150},
        "Books": {"brands": ["OReilly", "Pearson"], "base_price": 40},
        "Clothing": {"brands": ["Nike", "Adidas", "Levis"], "base_price": 60},
    }

    categories = list(category_brand_map.keys())
    products = []

    # To create a correlation for the ML model, we'll simulate that 
    # certain categories get better ratings from "high loyalty" users.
    for i in range(1, n + 1):
        category = random.choice(categories)
        brand = random.choice(category_brand_map[category]["brands"])
        base_price = category_brand_map[category]["base_price"]
        
        # Price logic: Add some variance based on the brand's "prestige"
        price_variance = random.uniform(0.8, 1.5)
        final_price = round(base_price * price_variance, 2)

        products.append({
            "product_id": f"P{i:05d}",
            "product_name": f"{brand} Item {i}",
            "category": category,
            "brand": brand,
            "price": inject_null(final_price),
            # Rating logic: We will link this to User Loyalty in the training phase,
            # but here we provide a base quality score.
            "base_quality_index": round(random.uniform(0.5, 1.0), 2), 
            "stock_quantity": random.randint(0, 100),
        })
    return products

@app.get("/products")
async def get_products(count: int = TOTAL_RECORDS, category: Optional[str] = None):
    all_products = generate_products(count)
    if category:
        return [p for p in all_products if p["category"].lower() == category.lower()]
    return all_products