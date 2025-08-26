from pyspark.sql import SparkSession

if __name__ == "__main__":
    print("--- Starting Spark Metastore Connection Test ---")
    
    spark = (
        SparkSession.builder
        .appName("MetastoreConnectionTest")
        .enableHiveSupport()
        .getOrCreate()
    )
    
    print("Spark Session created. Attempting to show databases...")
    
    # This command will fail if Spark cannot connect to the Hive Metastore
    spark.sql("SHOW DATABASES").show()
    
    print("Successfully connected to the Hive Metastore and showed databases.")
    
    spark.stop()
    
    print("--- Spark Metastore Connection Test Succeeded ---")