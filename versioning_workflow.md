Data Versioning with Git LFS
Storage Strategy: All raw and intermediate datasets (CSV files) are tracked using Git LFS to maintain repository performance.

Tracking: We use git lfs track "data/**/*.csv" to ensure large data files are offloaded from standard Git history.

Verification: To ensure LFS is functioning, we run git lfs ls-files to confirm that all data files are correctly managed by the LFS pointer system.

Synchronization: Before running the Airflow pipeline, we perform a git lfs pull to ensure the local workspace has the latest data versions.