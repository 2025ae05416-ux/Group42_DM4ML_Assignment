import pandas as pd
import numpy as np
import os
from dags.utils.config import BASE_DATA_DIR

def inject_nulls(series, rate=0.05):
    """Randomly set values to NaN at the given rate."""
    mask = np.random.rand(len(series)) < rate
    return series.where(~mask, other=np.nan)

def inject_duplicates(df, n=1):
    """Duplicate n random rows and append them."""
    dupes = df.sample(n=min(n, len(df)), replace=False)
    return pd.concat([df, dupes], ignore_index=True)

def generate_all_dirtydatasets(
    n_users=50,
    n_products=30,
    n_orders=80,
    n_reviews=60,
    null_rate=0.05,
    seed=42
):
    np.random.seed(seed)
    os.makedirs(BASE_DATA_DIR, exist_ok=True)

    # -------------------------
    # 1. USERS
    # -------------------------
    valid_genders = ["Male", "Female", "Other", "Non-binary"]
    dirty_genders = ["Robot", "Attack Helicopter", "Unknown", "N/A"]

    users = pd.DataFrame({
        "user_id": [f"U{i}" for i in range(1, n_users + 1)],
        "name": [f"User_{i}" for i in range(1, n_users + 1)],
        "email": [f"user{i}@test.com" for i in range(1, n_users + 1)],
        "gender": np.random.choice(valid_genders, n_users),
        "city": np.random.choice(["Mumbai", "Delhi", "Pune", "London", "Berlin"], n_users),
        "signup_date": pd.date_range("2025-01-01", periods=n_users, freq="3D").strftime("%Y-%m-%d"),
    })

    # Inject dirty gender values
    dirty_idx = np.random.choice(users.index, size=max(1, n_users // 10), replace=False)
    users.loc[dirty_idx, "gender"] = np.random.choice(dirty_genders, size=len(dirty_idx))

    # Inject nulls
    users["name"] = inject_nulls(users["name"], null_rate)
    users["email"] = inject_nulls(users["email"], null_rate)

    # Inject duplicates
    users = inject_duplicates(users, n=max(1, n_users // 20))
    users.to_csv(os.path.join(BASE_DATA_DIR, "users.csv"), index=False)

    # -------------------------
    # 2. PRODUCTS
    # -------------------------
    categories = ["Electronics", "Audio", "Clothing", "Books", "Kitchen"]
    brands = ["Dell", "HP", "Apple", "Sony", "Samsung", "Generic"]

    products = pd.DataFrame({
        "product_id": [f"P{i}" for i in range(101, 101 + n_products)],
        "product_name": [f"Product_{i}" for i in range(1, n_products + 1)],
        "category": np.random.choice(categories, n_products),
        "brand": np.random.choice(brands, n_products),
        "price": np.random.uniform(500, 100000, n_products).round(2),
        "rating": np.random.uniform(0, 5, n_products).round(1),
        "stock_quantity": np.random.randint(0, 200, n_products),
    })

    # Inject dirty prices (negative)
    dirty_price_idx = np.random.choice(products.index, size=max(1, n_products // 10), replace=False)
    products.loc[dirty_price_idx, "price"] = np.random.uniform(-1000, -1, size=len(dirty_price_idx)).round(2)

    # Inject dirty ratings (> 5)
    dirty_rating_idx = np.random.choice(products.index, size=max(1, n_products // 10), replace=False)
    products.loc[dirty_rating_idx, "rating"] = np.random.uniform(5.1, 10, size=len(dirty_rating_idx)).round(1)

    # Inject nulls
    products["product_name"] = inject_nulls(products["product_name"], null_rate)

    # Inject duplicates
    products = inject_duplicates(products, n=max(1, n_products // 20))
    products.to_csv(os.path.join(BASE_DATA_DIR, "products.csv"), index=False)

    # -------------------------
    # 3. ORDERS
    # -------------------------
    statuses = ["Delivered", "Shipped", "Pending", "Cancelled"]
    user_ids = [f"U{i}" for i in range(1, n_users + 1)]

    orders = pd.DataFrame({
        "order_id": [f"O{i}" for i in range(501, 501 + n_orders)],
        "user_id": np.random.choice(user_ids, n_orders),
        "order_date": pd.date_range("2026-01-01", periods=n_orders, freq="2D").strftime("%Y-%m-%d"),
        "order_status": np.random.choice(statuses, n_orders),
        "total_amount": np.random.uniform(100, 50000, n_orders).round(2),
    })

    # Inject future dates (dirty)
    future_idx = np.random.choice(orders.index, size=max(1, n_orders // 10), replace=False)
    orders.loc[future_idx, "order_date"] = pd.date_range(
        "2027-01-01", periods=len(future_idx), freq="5D"
    ).strftime("%Y-%m-%d")

    # Inject negative amounts (dirty)
    dirty_amt_idx = np.random.choice(orders.index, size=max(1, n_orders // 10), replace=False)
    orders.loc[dirty_amt_idx, "total_amount"] = np.random.uniform(-5000, -1, size=len(dirty_amt_idx)).round(2)

    # Inject nulls
    orders["order_status"] = inject_nulls(orders["order_status"], null_rate)

    # Inject duplicates
    orders = inject_duplicates(orders, n=max(1, n_orders // 20))
    orders.to_csv(os.path.join(BASE_DATA_DIR, "orders.csv"), index=False)

    # -------------------------
    # 4. REVIEWS
    # -------------------------
    product_ids = [f"P{i}" for i in range(101, 101 + n_products)]

    review_texts = [
        "Good product will buy again",
        "Not good wont buy at all",
        "Highly recommend this brand",
        "Color was different from images.",
        "Not as expected, quality is poor.",
        "Fast shipping and good packaging",
        "Value for money, satisfied with purchase.",
        "Terrible quality, returned immediately.",
        "Exceeded my expectations!",
        "Decent product but overpriced.",
    ]

    reviews = pd.DataFrame({
        "review_id": [f"R{i}" for i in range(9001, 9001 + n_reviews)],
        "order_id": np.random.choice([f"O{i}" for i in range(501, 501 + n_orders)], n_reviews),
        "product_id": np.random.choice(product_ids, n_reviews),
        "user_id": np.random.choice(user_ids, n_reviews),
        "rating": np.random.randint(1, 6, n_reviews),
        "review_text": np.random.choice(review_texts, n_reviews),
        "review_date": pd.date_range("2026-02-01", periods=n_reviews, freq="1D").strftime("%Y-%m-%d"),
    })

    # Inject dirty ratings (> 5)
    dirty_rev_idx = np.random.choice(reviews.index, size=max(1, n_reviews // 10), replace=False)
    reviews.loc[dirty_rev_idx, "rating"] = np.random.randint(6, 15, size=len(dirty_rev_idx))

    # Inject nulls
    reviews["review_text"] = inject_nulls(reviews["review_text"], null_rate)

    # Inject duplicates
    reviews = inject_duplicates(reviews, n=max(1, n_reviews // 20))
    reviews.to_csv(os.path.join(BASE_DATA_DIR, "reviews.csv"), index=False)

    print(f"Dynamic dirty datasets generated in {BASE_DATA_DIR}")
    print(f"  users: {len(users)} rows | products: {len(products)} rows | orders: {len(orders)} rows | reviews: {len(reviews)} rows")