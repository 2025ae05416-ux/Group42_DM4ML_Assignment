import os
import glob
import shutil
from datetime import datetime
from zoneinfo import ZoneInfo
from dags.utils.config import BASE_DATA_DIR

def archive_file(file_name, ds_id):
    now = datetime.now(ZoneInfo("Asia/Kolkata"))
    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")
    run_id = now.strftime("%Y%m%d_%H%M%S")

    # --- Directory Logic ---
    raw_dest_dir = os.path.join(BASE_DATA_DIR, "raw", ds_id, year, month, day, run_id)
    prepared_dest_dir = os.path.join(BASE_DATA_DIR, "prepared", ds_id, year, month, day, run_id)  # ✅ new
    transformed_dest_dir = os.path.join(BASE_DATA_DIR, "transformed", ds_id, year, month, day, run_id)
    metadata_dest_dir = os.path.join(BASE_DATA_DIR, "transformed_metadata", ds_id, year, month, day, run_id)

    # Create directories
    os.makedirs(raw_dest_dir, exist_ok=True)
    os.makedirs(prepared_dest_dir, exist_ok=True)  # ✅ new
    os.makedirs(transformed_dest_dir, exist_ok=True)
    os.makedirs(metadata_dest_dir, exist_ok=True)

    # --- Source Path Definitions ---
    raw_source = os.path.join(BASE_DATA_DIR, file_name)                        # e.g. reviews.csv
    prepared_source = os.path.join(BASE_DATA_DIR, f"prepared_{ds_id}.csv")     # ✅ new
    transformed_source = os.path.join(BASE_DATA_DIR, f"transformed_{ds_id}.csv")

    # --- Move raw file ---
    if os.path.exists(raw_source):
        shutil.move(raw_source, os.path.join(raw_dest_dir, file_name))
        print(f"Moved raw file to: {raw_dest_dir}")
    else:
        print(f"Warning: Raw file not found: {raw_source}")

    # --- Move prepared file ---  ✅ new
    if os.path.exists(prepared_source):
        shutil.move(prepared_source, os.path.join(prepared_dest_dir, f"prepared_{ds_id}.csv"))
        print(f"Moved prepared file to: {prepared_dest_dir}")
    else:
        print(f"Warning: Prepared file not found: {prepared_source}")

    # --- Move transformed file ---
    if os.path.exists(transformed_source):
        shutil.move(transformed_source, os.path.join(transformed_dest_dir, f"transformed_{ds_id}.csv"))
        print(f"Moved transformed file to: {transformed_dest_dir}")
    else:
        print(f"Warning: Transformed file not found: {transformed_source}")

    def move_if_exists(source, destination):
        if os.path.exists(source):
            dest_path = os.path.join(destination, os.path.basename(source))
            shutil.move(source, dest_path)
            print(f"Moved {source} -> {dest_path}")
        else:
            print(f"Skipping missing file: {source}")

    # Move files
    move_if_exists(raw_source, raw_dest_dir)
    move_if_exists(prepared_source, prepared_dest_dir)
    move_if_exists(transformed_source, transformed_dest_dir)

    # --- Metadata sidecars ---
    metadata_pattern = os.path.join(BASE_DATA_DIR, f"*{ds_id}*.metadata*")
    metadata_files = glob.glob(metadata_pattern)

    if not metadata_files:
        print(f"No metadata files found for {ds_id}")

    for meta_file in metadata_files:
        basename = os.path.basename(meta_file)
        parts = basename.split(".")
        # Preserving extension logic (e.g., metadata.json)
        metadata_ext = ".".join(parts[2:])
        new_name = f"transformed_{ds_id}_{run_id}.csv.{metadata_ext}"
        dest_path = os.path.join(metadata_dest_dir, new_name)
        
        shutil.move(meta_file, dest_path)
        
        if os.path.exists(meta_file):
            print(f"CRITICAL: Failed to remove {meta_file} after move!")
        else:
            print(f"Successfully archived metadata: {new_name}")