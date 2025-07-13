# https://airflow.apache.org/docs/docker-stack/build.html
FROM apache/airflow:3.0.1

# Switch to root user to install dependencies
USER root

# Combine package list update and installation in a single RUN command
# This avoids issues with cached layers and is more efficient.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    # Install Java and other necessary packages
    openjdk-17-jdk \
    procps && \
    # Clean up apt caches to reduce image size
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set environment variables for Java and Spark
ENV JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"
ENV SPARK_HOME="/opt/spark"
ENV PATH="${JAVA_HOME}/bin:${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"
ENV SPARK_MASTER_PORT="7077"
ENV SPARK_MASTER_HOST="spark-master"

# Create Spark directory
RUN mkdir -p ${SPARK_HOME}

# Download and extract a valid version of Spark
# The version 3.5.5 in your original file does not exist. Here we use 3.5.1.
# Check for the latest version at: https://spark.apache.org/downloads.html
RUN curl https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz -o spark.tgz && \
    tar -xvzf spark.tgz --directory ${SPARK_HOME} --strip-components 1 && \
    rm spark.tgz

# Copy custom Spark configuration
# COPY ./spark-defaults.conf "${SPARK_HOME}/conf"

# Set ownership of the Spark directory to the airflow user
RUN chown -R airflow ${SPARK_HOME}

# Switch back to the non-privileged airflow user
USER airflow

# Copy your local requirements file and install Python packages
COPY --chown=airflow:airflow requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt