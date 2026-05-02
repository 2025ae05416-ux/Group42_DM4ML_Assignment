
import os

from dags.utils.reporting import generate_pdf_quality_report
from dags.utils.config import BASE_DATA_DIR,DATA_FILE_MAP
from datetime import datetime
from zoneinfo import ZoneInfo


def plot_fn(ds_id, **kwargs):
    import pandas as pd
    import matplotlib.pyplot as plt
    import seaborn as sns
    from dags.utils.config import DATA_FILE_MAP
    from dags.utils.validation import validate_data
    
    file_path = DATA_FILE_MAP.get(ds_id)
    """
    Reads transformed data, generates visualization,
    and creates a data quality PDF report.
    """

    #file_path = os.path.join(BASE_DATA_DIR, f"transformed_{ds_id}.csv")

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Transformed file not found at {file_path}")

    df = pd.read_csv(file_path)

    # Directories
    plot_dir = os.path.join(BASE_DATA_DIR, "plots")
    
    pdf_dir = os.path.join(BASE_DATA_DIR, "reports")

    os.makedirs(plot_dir, exist_ok=True)
    os.makedirs(pdf_dir, exist_ok=True)

    #timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    timestamp = datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%Y%m%d_%H%M%S")
    
    # -----------------------------
    # Data Validation Call
    # -----------------------------
    # This now catches Missing values, duplicates, and range errors (1-5)
    stats, errors = validate_data(df, ds_id)

    # -----------------------------
    # Plot Generation
    # -----------------------------
    plt.figure(figsize=(10, 6))

    if ds_id == "users" and "gender" in df.columns:
        sns.countplot(data=df, x="gender")
        plt.title("User Gender Distribution")

    elif ds_id == "products" and "price" in df.columns:
        sns.histplot(data=df, x="price", bins=30)
        plt.title("Product Price Distribution")

    elif ds_id == "reviews" and "rating" in df.columns:
        # Visualizing the 1-5 range check
        sns.histplot(data=df, x="rating", discrete=True, bins=5)
        plt.title("Review Rating Distribution (Expected 1-5)")

    else:
        df.head(10).plot(kind="bar", subplots=True)

    plot_filename = f"{ds_id}_plot_{timestamp}.png"

    plot_path = os.path.join(plot_dir, plot_filename)

    plt.savefig(plot_path)
    plt.close()

    print(f"Generated plot: {plot_path}")

    # -----------------------------
    # Quality Checks
    # -----------------------------
    stats = {
        "total_rows": len(df),
        "duplicates": int(df.duplicated().sum()),
        "missing_values": int(df.isnull().sum().sum()),
    }

    errors = []

    if stats["duplicates"] > 0:
        errors.append("Duplicate rows detected")

    if stats["missing_values"] > 0:
        errors.append("Missing values detected")

    # -----------------------------
    # PDF Quality Report
    # -----------------------------
    pdf_filename = f"{ds_id}_quality_report_{timestamp}.pdf"

    pdf_path = os.path.join(pdf_dir, pdf_filename)

    generate_pdf_quality_report(
        ds_id=ds_id, stats=stats, errors=errors, output_path=pdf_path
    )

    print(f"Generated quality report: {pdf_path}")
