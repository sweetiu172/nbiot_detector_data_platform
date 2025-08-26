# Dockerfile

# Use a slim Python base image for a smaller footprint
FROM python:3.10-slim

# Set the working directory inside the container
WORKDIR /app

ARG AWS_ACCESS_KEY_ID=minio_access_key \
    AWS_SECRET_KEY_ID=minio_secret_key
    
# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYSPARK_PYTHON=/usr/bin/python3

# Install system dependencies
# - default-jre is required for PySpark to run
# - git is useful for some pip installs
RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jre \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy and install Python dependencies
# This is done in a separate step to leverage Docker's layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy your feature repository and the entrypoint script into the container
COPY . .
COPY entrypoint.sh .

# # Make the entrypoint script executable
# RUN chmod +x entrypoint.sh

# # Set the entrypoint for the container
# ENTRYPOINT ["./entrypoint.sh"]
# Keep container alive
CMD ["tail", "-f", "/dev/null"]