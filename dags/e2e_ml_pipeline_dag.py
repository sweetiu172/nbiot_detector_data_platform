# dags/e2e_ml_pipeline_dag.py
from __future__ import annotations
import pendulum
import sys
import os

DAG_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(DAG_DIR)
sys.path.insert(0, PROJECT_ROOT)

from airflow.models.dag import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models.variable import Variable

from scripts.train_model import run_training
from scripts.evaluate_model import run_evaluation


PROJECT_PATH = "/opt/airflow/dags"
INGESTION_SCRIPT_PATH = f"{PROJECT_PATH}/scripts/ingest_raw_data.py"

# --- Python Callables for PythonOperators ---
def train_model_task(**kwargs):
    from scripts.train_model import run_training
    run_training(date_str=kwargs['ds'], **kwargs)

def evaluate_model_task(**kwargs):
    from scripts.evaluate_model import run_evaluation
    run_evaluation(date_str=kwargs['ds'], **kwargs)

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
    ingest_to_bronze  = SparkSubmitOperator(
        task_id="ingest_raw_data_to_bronze",
        application="/opt/airflow/scripts/ingest_raw_data.py",
        conn_id="spark_default",
        # master="spark://spark-master:7077",
        application_args=["--date", "{{ ds }}"],
        conf=spark_conf,
    )

    train_model = PythonOperator(
        task_id="train_model",
        python_callable=train_model_task,
    )
    
    evaluate_and_promote_model = PythonOperator(
        task_id="evaluate_and_promote_model",
        python_callable=evaluate_model_task,
    )
    
    materialize_to_online_store = BashOperator(
        task_id="materialize_features",
        bash_command=f"cd {PROJECT_PATH}/feature_repo && feast materialize-incremental {{ ds }}",
        doc_md="Syncs the latest features from the offline store to the online store."
    )

    # Define the dependency chain for the entire pipeline
    ingest_to_bronze >> train_model >> evaluate_and_promote_model >> materialize_to_online_store