# scripts/ingest_raw_data.py
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

def run_job(spark, date_str):
    """
    Reads daily raw CSVs from the landing zone, transforms them,
    and appends them to the main processed Delta table.
    """
    # Construct the input path based on the date partition
    input_path = f"s3a://landing/dt={date_str}/*/*.csv"
    print(f"Reading raw data from: {input_path}")
    
    # Read the partitioned CSV data for all devices for the given day
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)
    
    # Add the date column to the DataFrame for partitioning in the final table
    df = df.withColumn("dt", lit(date_str))
    print("Added date column for partitioning.")
    
    # Define the path to the main processed Delta table
    output_path = "s3a://processed/iot_botnet_data"
    
    # Use 'append' mode to add the new day's data to the existing table
    print(f"Appending data to Delta table at: {output_path}")
    df.write.format("delta").mode("append").save(output_path)
    
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