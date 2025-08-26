from __future__ import annotations
import pendulum

from airflow.models.dag import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models.variable import Variable

PYTHONPATH = "/opt/airflow"
PROJECT_PATH = "/opt/airflow/dags"
INGESTION_SCRIPT_PATH = f"{PROJECT_PATH}/scripts/ingest_raw_data.py"

# Define the Spark configuration once
spark_conf = {
    "spark.hadoop.fs.s3a.endpoint": Variable.get("minio_endpoint"),
    "spark.hadoop.fs.s3a.access.key": Variable.get("minio_access_key"),
    "spark.hadoop.fs.s3a.secret.key": Variable.get("minio_secret_key"),
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.sql.session.timeZone": "UTC",
    "spark.hadoop.datanucleus.schema.autoCreateTables": "true",
    "spark.sql.warehouse.dir": "s3a://warehouse/spark-warehouse",
    "spark.jars": "/opt/spark/extra-jars/*",
    "spark.driver.extraClassPath": "/opt/spark/extra-jars/*",
    "spark.executor.extraClassPath": "/opt/spark/extra-jars/*"
}

with DAG(
    dag_id="daily_data_ingestion_pipeline",
    start_date=pendulum.datetime(2025, 7, 14, tz="UTC"),
    catchup=False,
    schedule="@daily",
    tags=["iot_botnet"],
) as dag:
    # test_task = SparkSubmitOperator(
    #     task_id="test_metastore_connection",
    #     application="/opt/airflow/scripts/test_metastore_connection.py", # Point to the new script
    #     conn_id="spark_default",
    #     conf=spark_conf,
    #     # All other configurations will be inherited from spark-defaults.conf
    #     # and the Airflow connection's "extra" field.
    # )
    
    # Task 1: Ingest raw data from the landing zone to the processed table
    ingest_to_bronze  = SparkSubmitOperator(
        task_id="ingest_raw_data_to_bronze",
        application="/opt/airflow/scripts/ingest_raw_data.py",
        conn_id="spark_default",
        # master="spark://spark-master:7077",
        application_args=["--date", "{{ ds }}"],
        conf=spark_conf,
    )

    ingest_to_bronze