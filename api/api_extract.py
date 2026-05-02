from fastapi import FastAPI
import random
from faker import Faker
from typing import Optional

app = FastAPI()
fake = Faker()

# Configuration for scaling and "dirtiness"
TOTAL_RECORDS = 2000
NULL_PROBABILITY = 0.1  # 10% chance for a field to be null

def inject_null(value):
    """Randomly returns None instead of the value based on NULL_PROBABILITY."""
    return value if random.random() > NULL_PROBABILITY else None

# --- USERS GENERATOR ---
def generate_users(n=TOTAL_RECORDS):
    data = []
    for i in range(1, n + 1):
        data.append({
            "user_id": f"U{i:06d}",
            "name": inject_null(fake.name()),
            "email": f"user{i:06d}@example.com",
            "gender": inject_null(random.choice(["Male", "Female", "Other"])),
            "city": inject_null(fake.city()),
            "signup_date": fake.date_between(start_date='-2y', end_date='today').strftime('%Y-%m-%d')
        })
    return data

@app.get("/users")
async def get_users(count: int = TOTAL_RECORDS):
    return generate_users(count)

# --- PRODUCTS GENERATOR ---
def generate_products(n=TOTAL_RECORDS):
    category_brand_map = {
        "Electronics": {
            "brands": ["Apple", "Samsung", "Sony", "Dell", "HP"],
            "items": ["Laptop", "Smartphone", "Headphones", "Monitor", "Tablet"],
        },
        "Home & Garden": {
            "brands": ["Ikea", "Whirlpool", "Dyson", "Philips", "KitchenAid"],
            "items": ["Coffee Maker", "Vacuum", "Lamp", "Desk", "Air Purifier"],
        },
        "Books": {
            "brands": ["Penguin", "HarperCollins", "OReilly", "Pearson"],
            "items": ["Python Guide", "Data Science Handbook", "Machine Learning Book", "SQL Fundamentals"],
        },
        "Clothing": {
            "brands": ["Nike", "Adidas", "Levis", "Puma", "Uniqlo"],
            "items": ["T-Shirt", "Jeans", "Jacket", "Sneakers", "Hoodie"],
        },
    }

    categories = list(category_brand_map.keys())
    products = []

    for i in range(1, n + 1):
        category = random.choice(categories)
        brand = random.choice(category_brand_map[category]["brands"])
        item = random.choice(category_brand_map[category]["items"])

        products.append({
            "product_id": f"P{i:05d}",
            "product_name": f"{brand} {item}",
            "category": inject_null(category), # Nullable category
            "brand": brand,
            "price": inject_null(round(random.uniform(10.0, 500.0), 2)), # Nullable price
            "rating": round(random.uniform(1.0, 5.0), 1),
            "stock_quantity": random.randint(0, 100),
        })
    return products

@app.get("/products")
async def get_products(count: int = TOTAL_RECORDS, category: Optional[str] = None):
    all_products = generate_products(count)
    if category:
        # We filter from the generated pool
        return [p for p in all_products if p["category"] and p["category"].lower() == category.lower()]
    return all_products