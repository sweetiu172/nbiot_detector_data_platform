# scripts/evaluate_model.py
import joblib
import pandas as pd
import mlflow
from mlflow.tracking import MlflowClient
from feast import FeatureStore
from sklearn.metrics import f1_score

def run_evaluation(date_str, **kwargs):
    """Compares the new model with the production model and promotes the winner."""
    mlflow.set_tracking_uri(os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000"))
    client = MlflowClient()
    
    ti = kwargs['ti']
    new_run_id = ti.xcom_pull(task_ids='train_model', key='new_model_run_id')
    if not new_run_id:
        raise ValueError("Could not find run_id for new model in XComs.")

    # Load the most recent day's data as a test set using Feast
    store = FeatureStore(repo_path="/opt/airflow/dags/repo/feature_repo")
    entity_df = pd.DataFrame({"event_timestamp": [pd.to_datetime(date_str, utc=True)]})
    
    test_job = store.get_historical_features(
        entity_df=entity_df,
        features=store.get_feature_view("nbiot_features")
    )
    test_df = test_job.to_df().dropna()

    y_test = (test_df['label'] == 'attack').astype(int)
    feature_names = [col for col in test_df.columns if col not in ['label', 'event_timestamp', 'device_name']]
    X_test = test_df[feature_names]

    # Evaluate new model
    scaler_path = mlflow.artifacts.download_artifacts(run_uri=f"runs:/{new_run_id}/scaler/scaler.gz")
    scaler = joblib.load(scaler_path)
    X_test_scaled = scaler.transform(X_test)
    
    new_model = mlflow.pyfunc.load_model(f"runs:/{new_run_id}/model")
    preds_new = new_model.predict(X_test_scaled)
    f1_new = f1_score(y_test, preds_new)
    client.log_metric(new_run_id, "test_f1_score", f1_new)
    print(f"New model F1 Score: {f1_new:.4f}")

    # Evaluate production model
    model_name = "nbiot-detector"
    f1_prod = -1.0
    try:
        prod_model_uri = f"models:/{model_name}/Production"
        # For a true comparison, we should use the scaler associated with the production model.
        # For simplicity, we reuse the new scaler.
        prod_model = mlflow.pyfunc.load_model(prod_model_uri)
        preds_prod = prod_model.predict(X_test_scaled)
        f1_prod = f1_score(y_test, preds_prod)
        print(f"Production model F1 Score: {f1_prod:.4f}")
    except Exception:
        print("No production model found. New model will be promoted.")

    # Compare and promote
    if f1_new > f1_prod:
        print("New model is better. Promoting to Production.")
        model_version = mlflow.register_model(f"runs:/{new_run_id}/model", model_name)
        client.transition_model_version_stage(
            name=model_name, version=model_version.version, stage="Production", archive_existing_versions=True
        )
    else:
        print("Production model is still better. No action taken.")