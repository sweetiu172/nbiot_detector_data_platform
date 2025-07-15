# dags/end_to_end_ml_pipeline.py
from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models.variable import Variable

# Define the Spark configuration once
spark_conf = {
    "spark.hadoop.fs.s3a.endpoint": Variable.get("minio_endpoint"),
    "spark.hadoop.fs.s3a.access.key": Variable.get("minio_access_key"),
    "spark.hadoop.fs.s3a.secret.key": Variable.get("minio_secret_key"),
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.jars": "/opt/spark/extra-jars/*",
}

with DAG(
    dag_id="end_to_end_ml_pipeline",
    start_date=pendulum.datetime(2025, 7, 14, tz="UTC"),
    catchup=False,
    schedule="@daily",
    tags=["iot_botnet", "production-style"],
) as dag:
    # Task 1: Ingest raw data from the landing zone to the processed table
    ingest_task = SparkSubmitOperator(
        task_id="ingest_raw_data",
        application="/opt/airflow/scripts/ingest_raw_data.py",
        conn_id="spark_default",
        # master="spark://spark-master:7077",
        application_args=["--date", "{{ ds }}"],
        conf=spark_conf,
    )

    # Task 2: Preprocess the daily data and prepare it for training
    preprocess_task = SparkSubmitOperator(
        task_id="preprocess_for_training",
        application="/opt/airflow/scripts/preprocess.py",
        conn_id="spark_default",
        # master="spark://spark-master:7077",
        application_args=["--date", "{{ ds }}"],
        conf=spark_conf,
    )

    # Task 3: Train the PyTorch model using the preprocessed data
    train_task = BashOperator(
        task_id="train_pytorch_model",
        bash_command="python /opt/airflow/scripts/train.py",
    )

    # Define the dependency chain for the entire pipeline
    ingest_task >> preprocess_task >> train_task