import json
import datetime
import os
from zoneinfo import ZoneInfo # Standard library in Python 3.9+

class MetadataTracker:
    def __init__(self, output_path, user_id="system"):
        # Define the IST Timezone
        self.ist_tz = ZoneInfo("Asia/Kolkata")
        
        self.output_path = output_path
        self.metadata = {
            "provenance": {
                # Force IST for the initial ingestion timestamp
                "ingestion_date": datetime.datetime.now(self.ist_tz).isoformat(),
                "user_id": user_id,
                "source_file": ""
            },
            "transformations": [],
            "experiment": {
                "run_id": None,
                "parameters": {},
                "metrics": {},
                "logged_at": None
            }
        }

    def add_source(self, source_name):
        self.metadata["provenance"]["source_file"] = source_name

    def log_transformation(self, step_name, details):
        """Logs a transformation step with an IST timestamp."""
        self.metadata["transformations"].append({
            "step": step_name,
            "details": details,
            "timestamp": datetime.datetime.now(self.ist_tz).isoformat()
        })

    def log_experiment_details(self, experiment_report):
        """Logs MLflow experiment details and timestamps the entry in IST."""
        if not experiment_report or "experiment" not in experiment_report:
            return

        exp = experiment_report["experiment"]
        self.metadata["experiment"]["run_id"] = exp.get("run_id")
        self.metadata["experiment"]["parameters"].update(exp.get("parameters", {}))
        self.metadata["experiment"]["metrics"].update(exp.get("metrics", {}))
        
        # Add a record of when this model metadata was attached
        self.metadata["experiment"]["logged_at"] = datetime.datetime.now(self.ist_tz).isoformat()

    def save(self):
        # Ensure the filename ends in .metadata.json
        meta_path = f"{self.output_path}.metadata.json"
        with open(meta_path, 'w') as f:
            json.dump(self.metadata, f, indent=4)
        print(f"Metadata saved with IST timestamps at: {meta_path}")