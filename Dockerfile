# Use the official Airflow 3.2.1 image based on Python 3.13
FROM apache/airflow:3.2.1
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow/dags"
# Switch to the root user to perform installations
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpango-1.0-0 \
    libpangocairo-1.0-0 \
    libpango1.0-dev \
    libffi-dev \
    libharfbuzz-dev \
    libimagequant-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir openpyxl faker
# Switch back to the airflow user
USER airflow
# Install your required Python packages
RUN pip install --no-cache-dir weasyprint pandera pandas kaggle matplotlib seaborn scikit-learn joblib