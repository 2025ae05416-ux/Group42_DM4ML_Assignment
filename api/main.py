from fastapi import FastAPI
import pandas as pd
import random
from faker import Faker

app = FastAPI()
fake = Faker()

# Your updated function
def generate_users(n=100):
    data = []
    for i in range(1, n + 1):
        data.append({
            "user_id": f"U{i:06d}",
            "name": fake.name(),
            "email": f"user{i:06d}@example.com",
            "gender": random.choice(["Male", "Female", "Other"]),
            "city": fake.city(),
            "signup_date": fake.date_between(start_date='-2y', end_date='today').strftime('%d-%m-%Y')
        })
    return data

@app.get("/users")
async def get_users(count: int = 100):
    """
    Returns a list of synthetic users. 
    Usage: /users?count=100
    """
    return generate_users(count)

def generate_products(n=100):
    categories = ["Electronics", "Home & Garden", "Books", "Clothing"]
    products = []
    for i in range(1, n + 1):
        products.append({
            "product_id": f"P{i:05d}",
            "product_name": f"Product {i}",
            "category": random.choice(categories),
            "price": round(random.uniform(10.0, 500.0), 2),
            "stock_quantity": random.randint(0, 100)
        })
    return products

@app.get("/products")
async def get_products(category: str = None):
    """
    Returns a list of products. 
    Usage: /products or /products?category=Electronics
    """
    all_products = generate_products(100)
    if category:
        return [p for p in all_products if p["category"].lower() == category.lower()]
    return all_products