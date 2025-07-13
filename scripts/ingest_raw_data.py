# scripts/ingest_raw_data.py
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

def run_job(spark, date_str):
    """
    Reads daily raw CSVs from the landing zone, transforms them,
    and appends them to the main processed Delta table, partitioned by date.
    """
    input_path = f"s3a://landing/dt={date_str}/*/*.csv"
    print(f"Reading raw data from: {input_path}")

    df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)

    # Add the date column to the DataFrame to be used for partitioning
    df = df.withColumn("dt", lit(date_str))
    print("Added date column for partitioning.")

    output_path = "s3a://processed/iot_botnet_data"

    print(f"Appending data to partitioned Delta table at: {output_path}")
    # --- KEY CHANGE: Add the .partitionBy("dt") command ---
    (df.write
       .format("delta")
       .mode("append")
       .option("mergeSchema", "true") # Good practice when appending to partitioned tables
       .partitionBy("dt")
       .save(output_path)
    )

    print("Ingestion and transformation complete.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="The date partition to process in YYYY-MM-DD format.")
    args = parser.parse_args()
    
    spark = (
        SparkSession.builder.appName("Raw_to_Processed_Ingestion")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    
    run_job(spark, args.date)