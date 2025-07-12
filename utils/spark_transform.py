import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, regexp_extract, lit

# --- 1. Configure Spark Session ---
spark = (
    SparkSession.builder.appName("IoT_Botnet_Transformation")
    .master("local[*]")
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minio_access_key")
    .config("spark.hadoop.fs.s3a.secret.key", "minio_secret_key")
    .config("spark.hadoop.fs.s3a.path.style.access", True)
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4")
    .getOrCreate()
)
print("Spark session created successfully!")