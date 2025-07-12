from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, DoubleType

def main():
    """Main ETL script for processing IoT Botnet data."""
    spark = (
        SparkSession.builder.appName("IoT_Botnet_Preprocessing")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    print("Spark session created for preprocessing.")

    # --- 1. Load Data ---
    processed_path = "s3a://processed/iot_botnet_data"
    delta_df = spark.read.format("delta").load(processed_path)
    print("Loaded data from:", processed_path)

    # --- 2. Sanitize and Clean ---
    sanitized_df = delta_df
    for col_name in delta_df.columns:
        if "." in col_name:
            new_col_name = col_name.replace(".", "_")
            sanitized_df = sanitized_df.withColumnRenamed(col_name, new_col_name)
    clean_df = sanitized_df.na.drop()
    print("Sanitized column names and dropped nulls.")

    # --- 3. Feature Engineering Pipeline ---
    feature_cols = [c for c, t in clean_df.dtypes if t in ('int', 'double') and c != 'label']
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid="skip")
    scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures")
    label_indexer = StringIndexer(inputCol="label", outputCol="indexedLabel")
    pipeline = Pipeline(stages=[assembler, scaler, label_indexer])
    preprocessor_model = pipeline.fit(clean_df)
    training_ready_df = preprocessor_model.transform(clean_df)
    print("Applied Spark ML preprocessing pipeline.")

    # --- 4. Convert Vector to Array ---
    print("Converting feature vector to a simple array...")
    # Define the function to convert a vector to a list
    def vector_to_array(v):
        return v.toArray().tolist()

    # Register the function as a UDF
    vector_to_array_udf = udf(vector_to_array, ArrayType(DoubleType()))

    # Apply the UDF to create a new column
    final_df_with_vector = training_ready_df.withColumn("features_flat", vector_to_array_udf("scaledFeatures"))

    # Select only the columns needed for training
    final_df = final_df_with_vector.select("features_flat", "indexedLabel")


    # --- 5. Add a Date Partition and Save ---
    from pyspark.sql.functions import current_date

    # In a real pipeline, this date would come from the data itself.
    # For now, we'll use the current date to simulate daily data.
    final_df_with_date = final_df.withColumn("dt", current_date())
    print("Added date partition column.")

    # Save the data, partitioned by our new date column 'dt'
    feature_store_path = "s3a://processed/training_ready_features"
    (
        final_df_with_date.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("dt")
        .save(feature_store_path)
    )
    print(f"Successfully saved PARTITIONED training-ready data to: {feature_store_path}")


    spark.stop()

if __name__ == "__main__":
    main()