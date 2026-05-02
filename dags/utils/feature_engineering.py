import pandas as pd
import os
import dags.utils.config as config


from dags.utils.recommendation_features import (
    engineer_features,
    generate_recommendation_features,
    get_top_n_similar_items,
)


def feature_engineering_fn(ds_id):
    import gc
    import logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    DATA_PATH = config.BASE_DATA_DIR

    input_path = f"{DATA_PATH}/transformed_{ds_id}.csv"
    feature_dir = config.FEATURE_DIR
    os.makedirs(feature_dir, exist_ok=True)

    df = pd.read_csv(input_path)

    # --------------------------------------------------
    # REVIEWS: Recommendation Features
    # --------------------------------------------------
    if ds_id == "reviews":

        # 1. Interaction Features
        interaction_df = engineer_features(df)

        interaction_df.to_csv(
            os.path.join(feature_dir, f"interaction_features_{ds_id}.csv"), index=False
        )
        logger.info(
            f"Successfully saved interaction_features to: {feature_dir}/interaction_features_{ds_id}.csv"
        )

        # 2. Similarity Features
        sim_matrix = generate_recommendation_features(interaction_df)

        sim_df = get_top_n_similar_items(sim_matrix, n=5)

        sim_df.to_csv(
            os.path.join(feature_dir, f"item_similarity_{ds_id}.csv"), index=False
        )
        logger.info(
            f"Successfully saved item_similarity to: {feature_dir}/item_similarity_{ds_id}.csv"
        )

    # --------------------------------------------------
    # ORDERS: Behavioral Features
    # --------------------------------------------------
    elif ds_id == "orders":

        required_cols = ["user_id", "order_date", "total_amount"]

        missing = [c for c in required_cols if c not in df.columns]

        if missing:
            raise ValueError(f"Missing columns {missing}")

        df["order_date"] = pd.to_datetime(
            df["order_date"], format="mixed", dayfirst=True, errors="coerce"
        )

        today = pd.Timestamp.today()

        # Order Frequency
        order_freq = df.groupby("user_id")["order_id"].count().rename("order_frequency")

        # Average Spend
        avg_spend = (
            df.groupby("user_id")["total_amount"].mean().rename("avg_order_value")
        )

        # Total Spend
        total_spend = df.groupby("user_id")["total_amount"].sum().rename("total_spend")

        # Recency
        last_order = df.groupby("user_id")["order_date"].max()

        recency = (today - last_order).dt.days.rename("days_since_last_order")

        # Avg Days Between Orders
        df = df.sort_values(["user_id", "order_date"])

        df["days_between_orders"] = df.groupby("user_id")["order_date"].diff().dt.days

        avg_gap = (
            df.groupby("user_id")["days_between_orders"]
            .mean()
            .fillna(0)
            .rename("avg_days_between_orders")
        )

        # Combine features
        orders_features = pd.concat(
            [order_freq, avg_spend, total_spend, recency, avg_gap], axis=1
        ).reset_index()

        orders_features.to_csv(
            os.path.join(feature_dir, "features_orders.csv"), index=False
        )

    # --------------------------------------------------
    # USERS
    # --------------------------------------------------
    elif ds_id == "users":

        if "signup_date" not in df.columns:
            raise KeyError(f"Column signup_date missing")

        df["signup_date"] = pd.to_datetime(
            df["signup_date"],
            format="mixed",
            dayfirst=True,
            errors="coerce"
        )

        bad_dates = df["signup_date"].isna().sum()

        if bad_dates:
            logger.warning(
        f"{bad_dates} invalid signup dates coerced to NaT"
        )

        df["days_since_signup"] = (
            pd.Timestamp.today() - df["signup_date"]
        ).dt.days

        df[["user_id", "days_since_signup"]].to_csv(
            os.path.join(feature_dir, "features_users.csv"), index=False
        )

    # --------------------------------------------------
    # PRODUCTS
    # --------------------------------------------------
    elif ds_id == "products":

        if "category" not in df.columns:
            raise KeyError(f"Column category missing")

        df["category_code"] = df["category"].astype("category").cat.codes

        df[["product_id", "category_code", "price"]].to_csv(
            os.path.join(feature_dir, "features_products.csv"), index=False
        )

    else:
        raise ValueError(
            f"CRITICAL: No feature engineering logic defined for dataset: {ds_id}"
        )
    del df
    gc.collect()
