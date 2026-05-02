def preprocess_fn(df):
    from sklearn.preprocessing import StandardScaler, LabelEncoder
    
    df.columns = [
        c.lower().strip()
        .replace(" ","_")
        .replace("-","_")
        for c in df.columns
    ]

    if "user_id" in df.columns and "product_id" in df.columns:
        df.dropna(
            subset=["user_id","product_id"],
            inplace=True
        )

    if "price" in df.columns:
        df["price"]=df["price"].fillna(
            df["price"].median()
        )

        scaler=StandardScaler()
        df["price_scaled"]=scaler.fit_transform(
            df[["price"]]
        )

    if "category" in df.columns:
        le=LabelEncoder()
        df["category_encoded"]=le.fit_transform(
            df["category"].astype(str)
        )

    return df