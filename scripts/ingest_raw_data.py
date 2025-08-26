# scripts/ingest_raw_data.py
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, to_timestamp, current_timestamp, input_file_name, regexp_extract, col, trim
from delta.tables import *


def run_job(spark, date_str):
    input_path = f"s3a://landing/dt={date_str}/*/*.csv"
    df = spark.read.option("Header", "true").option("inferSchema", "true").csv(input_path)

    sanitized_df = df
    for column_name in df.columns:
        if "." in column_name:
            new_column_name = column_name.replace(".", "_")
            sanitized_df = sanitized_df.withColumnRenamed(column_name, new_column_name)
    print("Sanitized column names, replacing '.' with '_'.")

    final_df = sanitized_df.withColumn("filepath", input_file_name())
    final_df = final_df.withColumn("device_name", regexp_extract(col("filepath"), r"device_name=([^/]+)", 1))
    final_df = final_df.drop("filepath")
    print("Added 'device_name' column extracted from file path.")

    final_df = final_df.filter(
        (col("device_name").isNotNull()) & (trim(col("device_name")) != "")
    )
    print("Filtered out rows with empty or whitespace-only device_name.")

    # Add columns for partitioning and for Feast
    final_df = final_df.withColumn("dt", lit(date_str))
    final_df = final_df.withColumn("event_timestamp", to_timestamp("dt"))
    final_df = final_df.withColumn("created_timestamp", current_timestamp())

    output_path = "s3a://processed/iot_traffic_bronze"
    table_name = "nbiot_bronze_delta_table"

    (final_df.write
        .format("delta")
        .mode("append")
        .partitionBy("dt")
        .save(output_path))
    
    print(f"Successfully appended data to Delta path: {output_path}")

    # --- Part 3: Generate the Hive-compatible manifest ---
    # This is the critical step to make the Delta table visible to all Spark sessions.
    
    # Get a DeltaTable object for the path
    delta_table = DeltaTable.forPath(spark, output_path)
    
    # Generate a manifest file in a sub-directory
    manifest_path = f"{output_path}/_symlink_format_manifest/"
    delta_table.generate("symlink_format_manifest")
    
    print(f"Generated symlink manifest for Hive compatibility.")

    # --- Part 4: Create a Hive EXTERNAL table pointing to the manifest ---
    # This table definition is universally understood by Spark sessions.
    spark.sql(f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {table_name} (
            `MI_dir_L5_weight` DOUBLE,
            `MI_dir_L5_mean` DOUBLE,
            `MI_dir_L5_variance` DOUBLE,
            `MI_dir_L3_weight` DOUBLE,
            `MI_dir_L3_mean` DOUBLE,
            `MI_dir_L3_variance` DOUBLE,
            `MI_dir_L1_weight` DOUBLE,
            `MI_dir_L1_mean` DOUBLE,
            `MI_dir_L1_variance` DOUBLE,
            `MI_dir_L0_1_weight` DOUBLE,
            `MI_dir_L0_1_mean` DOUBLE,
            `MI_dir_L0_1_variance` DOUBLE,
            `MI_dir_L0_01_weight` DOUBLE,
            `MI_dir_L0_01_mean` DOUBLE,
            `MI_dir_L0_01_variance` DOUBLE,
            `H_L5_weight` DOUBLE,
            `H_L5_mean` DOUBLE,
            `H_L5_variance` DOUBLE,
            `H_L3_weight` DOUBLE,
            `H_L3_mean` DOUBLE,
            `H_L3_variance` DOUBLE,
            `H_L1_weight` DOUBLE,
            `H_L1_mean` DOUBLE,
            `H_L1_variance` DOUBLE,
            `H_L0_1_weight` DOUBLE,
            `H_L0_1_mean` DOUBLE,
            `H_L0_1_variance` DOUBLE,
            `H_L0_01_weight` DOUBLE,
            `H_L0_01_mean` DOUBLE,
            `H_L0_01_variance` DOUBLE,
            `HH_L5_weight` DOUBLE,
            `HH_L5_mean` DOUBLE,
            `HH_L5_std` DOUBLE,
            `HH_L5_magnitude` DOUBLE,
            `HH_L5_radius` DOUBLE,
            `HH_L5_covariance` DOUBLE,
            `HH_L5_pcc` DOUBLE,
            `HH_L3_weight` DOUBLE,
            `HH_L3_mean` DOUBLE,
            `HH_L3_std` DOUBLE,
            `HH_L3_magnitude` DOUBLE,
            `HH_L3_radius` DOUBLE,
            `HH_L3_covariance` DOUBLE,
            `HH_L3_pcc` DOUBLE,
            `HH_L1_weight` DOUBLE,
            `HH_L1_mean` DOUBLE,
            `HH_L1_std` DOUBLE,
            `HH_L1_magnitude` DOUBLE,
            `HH_L1_radius` DOUBLE,
            `HH_L1_covariance` DOUBLE,
            `HH_L1_pcc` DOUBLE,
            `HH_L0_1_weight` DOUBLE,
            `HH_L0_1_mean` DOUBLE,
            `HH_L0_1_std` DOUBLE,
            `HH_L0_1_magnitude` DOUBLE,
            `HH_L0_1_radius` DOUBLE,
            `HH_L0_1_covariance` DOUBLE,
            `HH_L0_1_pcc` DOUBLE,
            `HH_L0_01_weight` DOUBLE,
            `HH_L0_01_mean` DOUBLE,
            `HH_L0_01_std` DOUBLE,
            `HH_L0_01_magnitude` DOUBLE,
            `HH_L0_01_radius` DOUBLE,
            `HH_L0_01_covariance` DOUBLE,
            `HH_L0_01_pcc` DOUBLE,
            `HH_jit_L5_weight` DOUBLE,
            `HH_jit_L5_mean` DOUBLE,
            `HH_jit_L5_variance` DOUBLE,
            `HH_jit_L3_weight` DOUBLE,
            `HH_jit_L3_mean` DOUBLE,
            `HH_jit_L3_variance` DOUBLE,
            `HH_jit_L1_weight` DOUBLE,
            `HH_jit_L1_mean` DOUBLE,
            `HH_jit_L1_variance` DOUBLE,
            `HH_jit_L0_1_weight` DOUBLE,
            `HH_jit_L0_1_mean` DOUBLE,
            `HH_jit_L0_1_variance` DOUBLE,
            `HH_jit_L0_01_weight` DOUBLE,
            `HH_jit_L0_01_mean` DOUBLE,
            `HH_jit_L0_01_variance` DOUBLE,
            `HpHp_L5_weight` DOUBLE,
            `HpHp_L5_mean` DOUBLE,
            `HpHp_L5_std` DOUBLE,
            `HpHp_L5_magnitude` DOUBLE,
            `HpHp_L5_radius` DOUBLE,
            `HpHp_L5_covariance` DOUBLE,
            `HpHp_L5_pcc` DOUBLE,
            `HpHp_L3_weight` DOUBLE,
            `HpHp_L3_mean` DOUBLE,
            `HpHp_L3_std` DOUBLE,
            `HpHp_L3_magnitude` DOUBLE,
            `HpHp_L3_radius` DOUBLE,
            `HpHp_L3_covariance` DOUBLE,
            `HpHp_L3_pcc` DOUBLE,
            `HpHp_L1_weight` DOUBLE,
            `HpHp_L1_mean` DOUBLE,
            `HpHp_L1_std` DOUBLE,
            `HpHp_L1_magnitude` DOUBLE,
            `HpHp_L1_radius` DOUBLE,
            `HpHp_L1_covariance` DOUBLE,
            `HpHp_L1_pcc` DOUBLE,
            `HpHp_L0_1_weight` DOUBLE,
            `HpHp_L0_1_mean` DOUBLE,
            `HpHp_L0_1_std` DOUBLE,
            `HpHp_L0_1_magnitude` DOUBLE,
            `HpHp_L0_1_radius` DOUBLE,
            `HpHp_L0_1_covariance` DOUBLE,
            `HpHp_L0_1_pcc` DOUBLE,
            `HpHp_L0_01_weight` DOUBLE,
            `HpHp_L0_01_mean` DOUBLE,
            `HpHp_L0_01_std` DOUBLE,
            `HpHp_L0_01_magnitude` DOUBLE,
            `HpHp_L0_01_radius` DOUBLE,
            `HpHp_L0_01_covariance` DOUBLE,
            `HpHp_L0_01_pcc` DOUBLE,
            `label` STRING,
            `device_name` STRING,
            `event_timestamp` TIMESTAMP,
            `created_timestamp` TIMESTAMP
        )
        PARTITIONED BY (dt STRING)
        LOCATION '{manifest_path}'
    """)
    
    # Refresh the table to make sure the new partitions are visible
    spark.sql(f"MSCK REPAIR TABLE {table_name}")

    print(f"Successfully created or updated Hive table '{table_name}'.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    args = parser.parse_args()
    
    spark = (
        SparkSession.builder.appName("Raw_to_Processed_Ingestion")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .enableHiveSupport() 
        .getOrCreate()
    )
    run_job(spark, args.date)