# scripts/train_model.py
import os
import joblib
import lightgbm as lgb
import pandas as pd
import mlflow
import mlflow.lightgbm
from feast import FeatureStore
from pyspark.sql import SparkSession
from sklearn.metrics import f1_score
from sklearn.preprocessing import RobustScaler
from datetime import datetime, timedelta

def run_training(date_str, training_days=30, **kwargs):
    """Builds a training set from Feast, trains a model, and logs it to MLflow."""
    mlflow.set_tracking_uri(os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000"))
    mlflow.set_experiment(os.environ.get("MLFLOW_EXPERIMENT_NAME", "N-BaIoT Botnet Detection"))

    with mlflow.start_run() as run:
        store = FeatureStore(repo_path="/opt/airflow/dags/repo/feature_repo")
        
        # Build the entity dataframe to select a time window of data for training
        end_date = datetime.strptime(date_str, '%Y-%m-%d')
        start_date = end_date - timedelta(days=training_days)
        
        # We need a SparkSession to query the offline store (Delta Lake)
        spark = SparkSession.builder.appName("TrainingDataBuilder").getOrCreate()
        bronze_table_path = "s3a://processed/temp_transform"
        entity_df = (
            spark.read.format("delta").load(bronze_table_path)
            .where(f"dt >= '{start_date.strftime('%Y-%m-%d')}' AND dt <= '{date_str}'")
            .select("event_timestamp", "device_name", "label")
        ).toPandas()
        
        # Get historical features from Feast
        training_job = store.get_historical_features(
            entity_df=entity_df,
            features=store.get_feature_view("nbiot_features")
        )
        training_df = training_job.to_df().dropna()

        # Prepare data for LightGBM
        y_train = (training_df['label'] == 'attack').astype(int)
        feature_names = [col for col in training_df.columns if col not in ['label', 'event_timestamp', 'device_name']]
        X_train = training_df[feature_names]
        
        scaler = RobustScaler()
        X_train_scaled = scaler.fit_transform(X_train)

        # Train model and log parameters
        imbalance_ratio = y_train.value_counts().get(0, 0) / y_train.value_counts().get(1, 1)
        mlflow.log_params({"training_days": training_days, "scale_pos_weight": imbalance_ratio})
        
        lgbm = lgb.LGBMClassifier(objective='binary', scale_pos_weight=imbalance_ratio, n_jobs=-1)
        lgbm.fit(X_train_scaled, y_train)
        
        # Log metrics and artifacts
        preds = lgbm.predict(X_train_scaled)
        mlflow.log_metric("training_f1_score", f1_score(y_train, preds))
        mlflow.lightgbm.log_model(lgbm, "model")
        
        joblib.dump(scaler, "scaler.gz")
        mlflow.log_artifact("scaler.gz", "scaler")
        
        kwargs['ti'].xcom_push(key='new_model_run_id', value=run.info.run_id)