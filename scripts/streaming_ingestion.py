from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, date_format
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

def run_streaming_job(spark):
    """
    Reads Debezium events from Kafka and writes them to a Delta Lake table.
    """
    # Define the schema for the "after" field in the Debezium JSON message
    payload_schema = StructType([
        # IMPORTANT: Add all 115 of your features here as StructField
        StructField("MI_dir_L5_weight", DoubleType(), True),
        # ... and so on ...
        StructField("label", StringType(), True),
        StructField("device_name", StringType(), True)
    ])
    debezium_schema = StructType([StructField("after", payload_schema, True)])

    # Read from the Kafka topic with a unique consumer group ID
    kafka_df = (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "broker:29092")
        .option("subscribe", "iot.public.iot_events")
        .option("kafka.group.id", "ingestion_consumer_group")
        .load())
    
    parsed_df = (kafka_df
                 .select(from_json(col("value").cast("string"), debezium_schema).alias("data"))
                 .select("data.after.*")
                 .filter(col("label").isNotNull()))
    
    final_df = (parsed_df
                .withColumn("event_timestamp", current_timestamp())
                .withColumn("created_timestamp", current_timestamp())
                .withColumn("dt", date_format(col("event_timestamp"), "yyyy-MM-dd")))
    
    output_path = "s3a://processed/iot_traffic_bronze"
    checkpoint_path = "s3a://processed/_checkpoints/ingestion_job"
    
    query = (final_df.writeStream
        .format("delta")
        .outputMode("append")
        .partitionBy("dt")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime='1 minute')
        .start())
        
    query.awaitTermination()

if __name__ == "__main__":
    spark = SparkSession.builder.appName("IoT_CDC_Ingestion_Spark").getOrCreate()
    run_streaming_job(spark)