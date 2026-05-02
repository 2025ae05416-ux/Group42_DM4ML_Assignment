import subprocess
import sys
import os
import pandas as pd

def generate_pdf_report(report, output_path):
    
    from fpdf import FPDF
    exp = report["experiment"]

    class PDF(FPDF):
        def header(self):
            # Blue Header bar
            self.set_fill_color(30, 144, 255)  # Dodger Blue
            self.rect(0, 0, 210, 30, 'F')
            self.set_text_color(255, 255, 255)
            self.set_font("Arial", 'B', 18)
            self.cell(0, 15, "MODEL PERFORMANCE REPORT", ln=True, align='C')
            self.ln(10)

        def section_header(self, label):
            self.ln(5)
            self.set_font("Arial", 'B', 12)
            self.set_text_color(30, 144, 255)
            self.cell(0, 10, label.upper(), ln=True)
            self.set_draw_color(30, 144, 255)
            self.line(self.get_x(), self.get_y(), self.get_x() + 190, self.get_y())
            self.ln(2)

        def data_row(self, key, value, fill):
            self.set_font("Arial", '', 10)
            self.set_text_color(0, 0, 0)
            self.set_fill_color(240, 240, 240) if fill else self.set_fill_color(255, 255, 255)
            # Create a "table" look
            self.cell(60, 8, f" {key}", border='B', fill=True)
            self.cell(130, 8, f" {value}", border='B', fill=True, ln=True)

    pdf = PDF()
    pdf.add_page()
    
    # --- Metadata Section ---
    pdf.section_header("General Run Information")
    pdf.data_row("Run ID", exp['run_id'], True)
    pdf.data_row("Dataset", exp['dataset'], False)
    pdf.data_row("Model Architecture", exp['model'], True)

    # --- Parameters Section ---
    pdf.section_header("Model Hyperparameters")
    fill = False
    for k, v in exp["parameters"].items():
        pdf.data_row(k.replace('_', ' ').title(), str(v), fill)
        fill = not fill

    # --- Metrics Section ---
    pdf.section_header("Evaluation Metrics")
    fill = False
    for k, v in exp["metrics"].items():
        # Highlighting the value if it's accuracy
        val_str = f"{v}%" if "accuracy" in k else str(v)
        pdf.data_row(k.replace('_', ' ').upper(), val_str, fill)
        fill = not fill

    pdf.output(output_path)

def train_and_evaluate(data_path, ds_id):
    import mlflow
    import numpy as np
    import datetime
    from sklearn.decomposition import TruncatedSVD
    from sklearn.metrics import mean_squared_error, mean_absolute_error
    from zoneinfo import ZoneInfo

    #mlflow.set_tracking_uri("http://mlflow_server:5000")
    mlflow.set_tracking_uri("http://172.18.0.6:5000")
    mlflow.set_experiment(f"RecSys_{ds_id}")
    
    with mlflow.start_run() as run:
        # 1. Load and Prepare Data
        df = pd.read_csv(data_path)
        categorical_cols = ['user_id', 'product_id']
        numeric_cols = ['user_activity_freq', 'user_avg_rating', 'item_avg_rating']
        matrix = df.pivot_table(
            index="user_id",
            columns="product_id",
            values="rating",
            aggfunc="mean"
        ).fillna(0)
        R = matrix.values
        # ---------------------------------
        # Train/test split on observed ratings
        # ---------------------------------
        observed = np.argwhere(R > 0)

        np.random.seed(42)
        np.random.shuffle(observed)
        test_size = int(0.2 * len(observed))
        test_idx = observed[:test_size]
        train_R = R.copy()

        # Mask test ratings from training matrix
        for i,j in test_idx:
            train_R[i,j] = 0


        # ---------------------------------
        # Train SVD
        # ---------------------------------

        n_components = max(
            1,
            min(train_R.shape[0]-1,
                train_R.shape[1]-1,
                20)
        )
        
        svd = TruncatedSVD(n_components=n_components)
        latent = svd.fit_transform(train_R)
        reconstructed = svd.inverse_transform(latent)
        # ---------------------------------
        # Evaluate only on held-out ratings
        # ---------------------------------
        y_true=[]
        y_pred=[]

        for i,j in test_idx:
            y_true.append(R[i,j])
            y_pred.append(reconstructed[i,j])

        y_pred = np.clip(y_pred, 1, 5)
        rmse=np.sqrt(
            mean_squared_error(y_true,y_pred)
        )

        mae=mean_absolute_error(
            y_true,y_pred
        )       
        accuracy_perc = max(0, 100 * (1 - (rmse / 5.0)))
                                      
        params = {
            "Dataset ID": ds_id,
            "Feature Count": len(categorical_cols) + len(numeric_cols),
            "Categorical": ", ".join(categorical_cols), 
            "Numerical": ", ".join(numeric_cols)
        }

        # Metrics rounded for display
        metrics = {
            "rmse": round(float(rmse), 4), 
            "mae": round(float(mae), 4), 
            "accuracy_percentage": round(float(accuracy_perc), 2)
        }
        # 5. Generate PDF Report
        experiment_report = {
            "experiment": {
            "run_id": run.info.run_id,
            "dataset": ds_id,
            "model": "TruncatedSVD",
            "parameters": params,
            "metrics": metrics
            }
        }


        # 4. Track Metadata in MLflow
        
        #mlflow.log_params(experiment_report["experiment"]["parameters"])
        #mlflow.log_metrics(experiment_report["experiment"]["metrics"])
        mlflow.set_tag("dataset", ds_id)
        mlflow.set_tag("model", "TruncatedSVD")
        mlflow.log_params(params)
        mlflow.log_metrics(metrics)


        mlflow.log_dict(
            experiment_report,
            "experiment_report.json"
        )

        report_dir = "data/reports/mlmodel_reports"
        os.makedirs(report_dir, exist_ok=True)
        timestamp = datetime.datetime.now(
            ZoneInfo("Asia/Kolkata")
        ).strftime("%Y%m%d_%H%M%S")
        #timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

        pdf_path = os.path.join(
            report_dir,
            f"mlperformance_report_{ds_id}_{timestamp}.pdf"
        )
        generate_pdf_report(experiment_report, pdf_path)
        mlflow.log_artifact(pdf_path)
        # Return results dictionary for Airflow XCom capture
    return experiment_report