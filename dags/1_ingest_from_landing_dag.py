# dags/1_ingest_from_landing_dag.py
from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models.variable import Variable

with DAG(
    dag_id="1_ingest_from_landing_local",
    start_date=pendulum.datetime(2025, 7, 10, tz="UTC"),
    catchup=False,
    schedule="@daily",
    tags=["iot_botnet", "ingestion", "local"],
) as dag:
    # Read configuration from Airflow Variables (which are set by env vars)
    spark_conf = {
        "spark.hadoop.fs.s3a.endpoint": Variable.get("minio_endpoint"),
        "spark.hadoop.fs.s3a.access.key": Variable.get("minio_access_key"),
        "spark.hadoop.fs.s3a.secret.key": Variable.get("minio_secret_key"),
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.eventLog.enabled": "false",
        "spark.jars": "/opt/spark/extra-jars/*",
    }

    print("Start to run")

    ingest_and_transform_task = SparkSubmitOperator(
        task_id="ingest_and_transform_raw_data",
        application="/opt/airflow/scripts/ingest_raw_data.py",
        conn_id="spark_default", # This connects to spark://spark-master:7077
        # Pass the execution date to the Spark script
        application_args=["--date", "{{ ds }}"],
        conf=spark_conf,
    )