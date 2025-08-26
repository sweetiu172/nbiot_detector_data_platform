# ./scripts/check_table_data.py

import os
from pyspark.sql import SparkSession
from pyspark.errors.exceptions.captured import AnalysisException

def check_table():
    """
    Creates a Spark session, connects to the Hive Metastore,
    and checks for data in the nbiot_bronze_delta_table.
    """
    print("--- Starting Table Data Check ---")

    # --- Use your local, pre-downloaded JARs ---
    # Create a comma-separated list of all JARs in the directory.
    # Assumes the script is run from a context where /app/jars exists.
    jar_dir = "/app/jars" # This path is inside the container
    try:
        jar_files = [os.path.join(jar_dir, f) for f in os.listdir(jar_dir) if f.endswith('.jar')]
        spark_jars_list = ",".join(jar_files)
        print(f"Found {len(jar_files)} JARs to load.")
    except FileNotFoundError:
        print(f"Error: JAR directory '{jar_dir}' not found. Please ensure your JARs are correctly mounted.")
        return

    # --- Build the Spark Session ---
    try:
        spark = (
            SparkSession.builder.appName("TableDataCheck")
            .master("spark://spark-master:7077")
            # S3A Config for MinIO
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY", "minio_access_key"))
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY", "minio_secret_key"))
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            # Hive Metastore Config for PostgreSQL
            .config("spark.hadoop.javax.jdo.option.ConnectionURL", "jdbc:postgresql://postgres-metastore:5432/metastore")
            .config("spark.hadoop.javax.jdo.option.ConnectionDriverName", "org.postgresql.Driver")
            .config("spark.hadoop.javax.jdo.option.ConnectionUserName", "hive")
            .config("spark.hadoop.javax.jdo.option.ConnectionPassword", "hive")
            .config("spark.hadoop.datanucleus.autoCreateSchema", "true")
            # Delta Lake and Warehouse Config
            .config("spark.sql.warehouse.dir", "s3a://warehouse/spark-warehouse")
            .config("spark.sql.catalogImplementation", "hive")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            # JARs and Classpath
            .config("spark.jars", spark_jars_list)
            .config("spark.driver.extraClassPath", f"{jar_dir}/*")
            .config("spark.executor.extraClassPath", f"{jar_dir}/*")
            .enableHiveSupport()
            .getOrCreate()
        )

        print("\n✅ Spark session created successfully.")

        # --- Check for the table and count the data ---
        table_name = "nbiot_bronze_delta_table"
        print(f"\nQuerying table: {table_name}")
        
        count_df = spark.sql(f"SELECT COUNT(*) as row_count FROM {table_name}")
        count_df.show()

        result = count_df.first()
        if result and result["row_count"] > 0:
            print(f"\n✅ SUCCESS: The table '{table_name}' exists and contains {result['row_count']} rows.")
        else:
            print(f"\n⚠️ The table '{table_name}' exists but is EMPTY.")

    except AnalysisException as e:
        if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
            print(f"\n❌ FAILED: The table '{table_name}' does not exist in the Hive Metastore.")
        else:
            print(f"\n❌ An unexpected Spark error occurred: {e}")
    except Exception as e:
        print(f"\n❌ An unexpected error occurred: {e}")
    finally:
        if 'spark' in locals():
            spark.stop()
        print("\n--- Table Data Check Finished ---")


if __name__ == "__main__":
    check_table()