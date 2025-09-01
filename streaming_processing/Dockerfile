# Use a slim Python base image for a smaller footprint
FROM python:3.10-slim-bookworm

# Versions for Spark and Hadoop
ARG SPARK_VERSION=3.5.0
ARG HADOOP_VERSION=3

# Set the working directory inside the container
WORKDIR /app

# Install dependencies: wget for downloading, Java for Spark, and procps.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jdk \
    procps \
    wget && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

    # Download and install Spark binaries. These are needed for the `spark-submit` client script.
RUN wget -qO- "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" | tar -xz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark

# Set environment variables for Java and Spark
ENV JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"
ENV SPARK_HOME="/opt/spark"
ENV PATH="${JAVA_HOME}/bin:${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

# Copy and install Python dependencies
# This is done in a separate step to leverage Docker's layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["tail", "-f", "/dev/null"]
