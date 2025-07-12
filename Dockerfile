# https://airflow.apache.org/docs/docker-stack/build.html
FROM apache/airflow:3.0.1

# Install the Airflow providers
USER airflow
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

# Switch to root to install packages and modify groups
USER root
RUN apt-get update && apt-get install -y default-jre-headless && apt-get clean

