# dags/1_ingest_from_landing_dag.py
from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.providers.docker.operators.docker import DockerOperator

# NOTE: Your docker-compose project name is used in the network_mode.
# By default it's the name of the folder your docker-compose.yml is in.
# Replace 'end-to-end-mlops' if your folder has a different name.
NETWORK_NAME = "end-to-end-mlops_default"

with DAG(
    dag_id="1_ingest_from_landing_local",
    start_date=pendulum.datetime(2025, 7, 12, tz="UTC"),
    catchup=False,
    schedule="@daily",
    tags=["iot_botnet", "ingestion", "local-docker"],
) as dag:
    # This command will be executed inside the new container
    spark_submit_command = """
        spark-submit \
            --master spark://spark-master:7077 \
            --packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4 \
            --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
            --conf spark.hadoop.fs.s3a.access.key=minio_access_key \
            --conf spark.hadoop.fs.s3a.secret.key=minio_secret_key \
            --conf spark.hadoop.fs.s3a.path.style.access=true \
            --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
            --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
            --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
            /home/jovyan/scripts/ingest_raw_data.py \
            --date {{ ds }}
    """

    ingest_and_transform_task = DockerOperator(
        task_id="ingest_and_transform_job",
        image="jupyter/pyspark-notebook:spark-3.5.0",  # The same image as our Spark cluster
        command=spark_submit_command,
        docker_url="unix://var/run/docker.sock",      # Connect to the Docker daemon on the host
        network_mode=NETWORK_NAME,                   # Connect to the same network as our other services
        auto_remove="success",                            # Clean up the container after it runs
    )