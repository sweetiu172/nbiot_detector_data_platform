# dags/2_preprocess_and_train_dag.py
from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models.variable import Variable

with DAG(
    dag_id="2_preprocess_and_train",
    start_date=pendulum.datetime(2025, 7, 12, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=["iot_botnet", "training"],
) as dag:
    spark_conf = {
        "spark.hadoop.fs.s3a.endpoint": Variable.get("minio_endpoint"),
        "spark.hadoop.fs.s3a.access.key": Variable.get("minio_access_key"),
        "spark.hadoop.fs.s3a.secret.key": Variable.get("minio_secret_key"),
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.eventLog.enabled": "false",
        "spark.jars": "/opt/spark/extra-jars/*",
    }


    wait_for_ingestion = ExternalTaskSensor(
        task_id="wait_for_ingestion",
        external_dag_id="1_ingest_from_landing_local",
        external_task_id="ingest_and_transform_raw_data",
        timeout=300,
        allowed_states=['success'],
        mode='reschedule',
        poke_interval=30,  
    )

    preprocess_task = SparkSubmitOperator(
        task_id="spark_preprocess_job",
        application="/opt/airflow/scripts/preprocess.py",
        conn_id="spark_default",
        application_args=["--date", "{{ ds }}"],
        conf=spark_conf
    )
    
    # Use BashOperator to run the training script directly in the Airflow container
    train_task = BashOperator(
        task_id="pytorch_model_training",
        bash_command="python /opt/airflow/scripts/train.py",
    )

    wait_for_ingestion >> preprocess_task >> train_task