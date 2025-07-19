FROM python:3.11-slim

# Install system dependencies needed by LightGBM
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgomp1 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/ml_scripts

# Copy and install only the ML-related python packages
COPY ./requirements-ml.txt .
RUN pip install --no-cache-dir -r requirements-ml.txt

# This container will be stateless, code will be mounted by Airflow
ENTRYPOINT [ "python" ]