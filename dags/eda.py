import os
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns

def generate_eda_report(df, ds_id):
    save_dir = "/opt/airflow/data/reports"
    os.makedirs(save_dir, exist_ok=True)
    
    # --- Part 1: Distribution Histogram ---
    dist_path = os.path.join(save_dir, f"{ds_id}_eda.png")
    try:
        fig, ax = plt.subplots(figsize=(10, 6))
        df.groupby('user_id')['product_id'].count().hist(bins=50, ax=ax)
        ax.set_title(f"Interaction Distribution for {ds_id}")
        fig.savefig(dist_path)
        plt.close(fig)
        print(f"SUCCESS: Histogram saved at {dist_path}")
    except Exception as e:
        print(f"ERROR: Histogram failed: {e}")

    # --- Part 2: Heatmap ---
    heat_path = os.path.join(save_dir, f"{ds_id}_heatmap.png")
    try:
        plt.figure(figsize=(10, 6))
        pivot_df = df.pivot_table(index='user_id', columns='product_id', aggfunc='size', fill_value=0)
        sns.heatmap(pivot_df > 0, cbar=False, cmap='Blues')
        plt.title(f"Sparsity Pattern (Heatmap) for {ds_id}")
        plt.savefig(heat_path)
        plt.close()
        print(f"SUCCESS: Heatmap saved at {heat_path}")
    except Exception as e:
        print(f"ERROR: Heatmap failed: {e}")