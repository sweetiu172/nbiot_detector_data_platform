# scripts/preprocess.py
import argparse
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.sql.functions import lit, udf, when
from pyspark.sql.types import ArrayType, DoubleType

def main(date_str):
    """Main ETL script for processing a specific partition of the IoT Botnet data."""
    spark = (
        SparkSession.builder.appName("IoT_Botnet_Preprocessing")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    print("Spark session created for preprocessing.")

    # --- 1. Load Specific Partition from the Processed Table ---
    processed_path = "s3a://processed/temp_transform"
    print(f"Reading partition dt={date_str} from {processed_path}")
    delta_df = spark.read.format("delta").load(processed_path).where(f"dt = '{date_str}'")

    # --- 2. Sanitize and Clean ---
    sanitized_df = delta_df
    for col_name in delta_df.columns:
        if "." in col_name:
            new_col_name = col_name.replace(".", "_")
            sanitized_df = sanitized_df.withColumnRenamed(col_name, new_col_name)
    clean_df = sanitized_df.na.drop()
    print("Sanitized column names and dropped nulls.")

    # --- 3. Feature Engineering Pipeline ---
    labeled_df = clean_df.withColumn(
        "indexedLabel",
        when(clean_df.label == "attack", 1.0).otherwise(0.0)
    )
    print("Replaced StringIndexer with explicit label mapping.")

    # --- Feature Engineering Pipeline (no longer includes StringIndexer) ---
    feature_cols = [c for c, t in labeled_df.dtypes if t in ('int', 'double') and c not in ['label', 'indexedLabel', 'dt']]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid="skip")
    scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures")
    pipeline = Pipeline(stages=[assembler, scaler])
    
    preprocessor_model = pipeline.fit(labeled_df)
    training_ready_df = preprocessor_model.transform(labeled_df)

    # --- Convert Vector and Save ---
    def vector_to_array(v):
        return v.toArray().tolist()
    vector_to_array_udf = udf(vector_to_array, ArrayType(DoubleType()))
    final_df_with_vector = training_ready_df.withColumn("features_flat", vector_to_array_udf("scaledFeatures"))

    final_df_with_date = final_df_with_vector.withColumn("dt", lit(date_str))
    final_df = final_df_with_date.select("features_flat", "indexedLabel", "dt")
    
    feature_store_path = "s3a://processed/training_ready_features"
    (final_df.write
        .format("delta")
        .mode("append")
        .partitionBy("dt")
        .option("mergeSchema", "true")
        .save(feature_store_path)
    )
    
    print(f"Successfully saved training-ready data to: {feature_store_path}")
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="The date partition to process in YYYY-MM-DD format.")
    args = parser.parse_args()
    main(args.date)