import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import mlflow
from feast import FeatureStore

# --- Global variables for the processing function ---
model = None
store = None

def enrich_and_predict_batch(batch_df, batch_id):
    """
    Called for each micro-batch. Enriches with Feast and makes predictions.
    Writes the enriched data with predictions to an intermediate Delta table.
    """
    print(f"--- [Batch ID: {batch_id}] Processing {batch_df.count()} events ---")
    
    global model, store
    if batch_df.count() == 0:
        return

    # 1. Load Model and Feature Store
    if model is None:
        # model = mlflow.pyfunc.load_model("models:/nbiot-detector/Production")
        run_id = "4954403c3b264009af21e5772897e7a1"  #  pasting your Run ID
        model_uri = f"runs:/{run_id}/model"
        print(f"Loading model from experimental run: {model_uri}")
        model = mlflow.pyfunc.load_model(model_uri)
        store = FeatureStore(repo_path="/app/feature_repo")

    # 2. Get device names from the micro-batch
    pandas_df = batch_df.toPandas()
    entity_rows = [{"device_name": row["device_name"]} for _, row in pandas_df.iterrows()]
    
    if not entity_rows:
        return

    # 3. Get the required feature list from the model's metadata
    feature_names = model.metadata.get_input_schema().input_names()
    # Format the list for Feast (e.g., "nbiot_features:MI_dir_L5_weight")
    features_to_fetch = [f"nbiot_features:{name}" for name in feature_names]

    # 4. Enrich events by fetching ONLY the required features from Feast
    feature_vector_df = store.get_online_features(
        features=features_to_fetch,
        entity_rows=entity_rows
    ).to_df()
    
    # 5. Predict using the correctly ordered columns
    predictions = model.predict(feature_vector_df[feature_names])
    # --- END OF CORRECTION ---
    
    # 6. Prepare DataFrame to save to the intermediate table
    feature_vector_df['prediction'] = predictions
    final_df_to_save = pd.merge(
        feature_vector_df[['device_name', 'prediction']], 
        pandas_df[['device_name', 'event_timestamp']], 
        on='device_name'
    )
    
    # 7. Write to intermediate Delta table
    spark = SparkSession.builder.getOrCreate()
    spark_df_to_save = spark.createDataFrame(final_df_to_save)
    spark_df_to_save.write.format("delta").mode("append").save("s3a://processed/predicted_events")


def run_alerting_pipeline(spark):
    """Sets up and runs the two-stage Spark streaming pipeline."""
    
    # --- STREAM 1: Read from Kafka, enrich with Feast, write to intermediate Delta table ---
    
    # Schema for the 'thin event' from Debezium
    payload_schema = StructType([StructField("device_name", StringType(), True)])
    debezium_schema = StructType([StructField("after", payload_schema, True)])

    kafka_df = (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "broker:29092")
        .option("subscribe", "iot.public.iot_events")
        .option("kafka.group.id", "alert_consumer_group")
        .option("startingOffsets", "earliest")
        .load())
    
    # parsed_df = (kafka_df
    #              .select(from_json(col("value").cast("string"), debezium_schema).alias("data"))
    #              .select("data.after.*", col("timestamp").alias("event_timestamp"))
    #              .filter(col("device_name").isNotNull()))
    parsed_df = (kafka_df
             .withColumn("data", from_json(col("value").cast("string"), debezium_schema))
             .select("data.after.*", col("timestamp").alias("event_timestamp"))
             .filter(col("device_name").isNotNull()))
    
    # This stream's only job is to write to the intermediate table
    _ = (parsed_df.writeStream
        .foreachBatch(enrich_and_predict_batch)
        .trigger(processingTime='10 seconds')
        .option("checkpointLocation", "s3a://processed/_checkpoints/enrichment")
        .start())

    # --- STREAM 2: Read from the intermediate Delta table, apply windowing, and alert ---
    
    # predicted_events_schema = StructType([
    #     StructField("device_name", StringType(), True),
    #     StructField("prediction", IntegerType(), True),
    #     StructField("event_timestamp", TimestampType(), True)
    # ])

    # predicted_stream_df = spark.readStream.format("delta").schema(predicted_events_schema).load("s3a://processed/predicted_events")
    predicted_stream_df = spark.readStream.format("delta").load("s3a://processed/predicted_events")
    windowed_counts = (predicted_stream_df
        .withWatermark("event_timestamp", "10 minutes")
        .groupBy(
            window(col("event_timestamp"), "5 minutes", "1 minute"),
            col("device_name")
        )
        .agg({"prediction": "sum"})
        .withColumnRenamed("sum(prediction)", "attack_count"))
    
    anomalous_windows = windowed_counts.filter(col("attack_count") > 10)
    
    # This stream's job is to write alerts to the console
    alerting_query = (anomalous_windows.writeStream
                      .outputMode("update")
                      .format("console")
                      .option("truncate", "false")
                      .start())
                      
    alerting_query.awaitTermination()

if __name__ == "__main__":
    os.environ["MLFLOW_TRACKING_URI"] = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
    spark = SparkSession.builder.appName("IoT_Feast_Windowed_Alerting").getOrCreate()
    run_alerting_pipeline(spark)