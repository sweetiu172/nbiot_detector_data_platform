# scripts/ingest_raw_data.py
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, to_timestamp, current_timestamp

def run_job(spark, date_str):
    input_path = f"s3a://landing/dt={date_str}/*/*.csv"
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)

    # Add columns for partitioning and for Feast
    df = df.withColumn("dt", lit(date_str))
    df = df.withColumn("event_timestamp", to_timestamp("dt"))
    df = df.withColumn("created_timestamp", current_timestamp())

    output_path = "s3a://processed/temp_transform"
    (df.write
       .format("delta")
       .mode("append")
       .option("mergeSchema", "true")
       .partitionBy("dt")
       .save(output_path))
    print("Ingestion to Bronze table complete.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    args = parser.parse_args()
    
    spark = (
        SparkSession.builder.appName("Raw_to_Processed_Ingestion")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    run_job(spark, args.date)