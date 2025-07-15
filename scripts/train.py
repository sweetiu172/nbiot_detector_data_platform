# scripts/train.py
import mlflow
import mlflow.pytorch
import torch
import torch.nn as nn
import numpy as np
from pyspark.sql import SparkSession

# --- Configuration ---
REGISTERED_MODEL_NAME = "iot-botnet-detector"
BATCH_SIZE = 256

def data_generator(spark_df, batch_size):
    """
    A Python generator to yield batches of data from a Spark DataFrame iterator.
    """
    iterator = spark_df.toLocalIterator()
    
    while True:
        # Collect rows for one batch
        batch_data = []
        for _ in range(batch_size):
            try:
                row = next(iterator)
                # Convert from Spark Row to a tuple of numpy arrays
                features = np.array(row.features_flat, dtype=np.float32)
                label = np.array([row.indexedLabel], dtype=np.float32)
                batch_data.append((features, label))
            except StopIteration:
                break # Reached the end of the data
        
        if not batch_data:
            break # Exit the while loop if no data was collected
        
        # Unzip the batch data into features and labels
        features_batch, labels_batch = zip(*batch_data)
        
        # Yield a batch as PyTorch tensors
        yield torch.tensor(np.array(features_batch)), torch.tensor(np.array(labels_batch))

def main():
    """
    Main training script that uses Spark's toLocalIterator for batching.
    """
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    print(f"Using device: {device}")

    # --- 1. Model Architecture ---
    class MLPDetector(nn.Module):
        def __init__(self, input_size, hidden_size_1, hidden_size_2, output_size, dropout_rate=0.3):
            super(MLPDetector, self).__init__()
            self.layer_1 = nn.Linear(input_size, hidden_size_1)
            self.relu_1 = nn.ReLU()
            self.dropout_1 = nn.Dropout(dropout_rate)
            self.layer_2 = nn.Linear(hidden_size_1, hidden_size_2)
            self.relu_2 = nn.ReLU()
            self.dropout_2 = nn.Dropout(dropout_rate)
            self.output_layer = nn.Linear(hidden_size_2, output_size)

        def forward(self, x):
            x = self.relu_1(self.dropout_1(self.layer_1(x)))
            x = self.relu_2(self.dropout_2(self.layer_2(x)))
            x = self.output_layer(x)
            return x

    # --- 2. MLflow Setup ---
    mlflow.set_tracking_uri("http://mlflow_server:5000")
    mlflow.set_experiment("iot_botnet_detection_iterator")

    with mlflow.start_run() as run:
        # --- 3. Hyperparameters & Model Loading ---
        LEARNING_RATE = 0.001
        EPOCHS = 5
        INPUT_SIZE = 115

        try:
            model_uri = f"models:/{REGISTERED_MODEL_NAME}/latest"
            model = mlflow.pytorch.load_model(model_uri).to(device)
            print(f"Loaded previous model from {model_uri} for continuous training.")
        except mlflow.exceptions.MlflowException:
            print(f"No registered model named '{REGISTERED_MODEL_NAME}' found. Training from scratch.")
            model = MLPDetector(input_size=INPUT_SIZE, hidden_size_1=128, hidden_size_2=64, output_size=1, dropout_rate=0.3).to(device)

        criterion = nn.BCEWithLogitsLoss()
        optimizer = torch.optim.Adam(model.parameters(), lr=LEARNING_RATE)
        mlflow.log_params({"learning_rate": LEARNING_RATE, "epochs": EPOCHS, "batch_size": BATCH_SIZE})

        # --- 4. Data Loading using Spark ---
        spark = SparkSession.builder.appName("TrainingDataLoader").getOrCreate()
        feature_store_path = "s3a://processed/training_ready_features"
        training_df = spark.read.format("delta").load(feature_store_path)
        print("Spark DataFrame loaded for training.")

        # --- 5. Training Loop ---
        print(f"Beginning training for {EPOCHS} epochs...")
        for epoch in range(EPOCHS):
            model.train()
            total_loss, num_batches = 0, 0
            
            # Create a new generator for each epoch
            train_loader = data_generator(training_df, BATCH_SIZE)
            
            for features, labels in train_loader:
                features = features.to(device)
                labels = labels.to(device)
                
                outputs = model(features)
                loss = criterion(outputs, labels)
                
                optimizer.zero_grad()
                loss.backward()
                optimizer.step()
                
                total_loss += loss.item()
                num_batches += 1
            
            if num_batches > 0:
                avg_loss = total_loss / num_batches
                print(f"Epoch [{epoch+1}/{EPOCHS}], Average Loss: {avg_loss:.4f}")
                mlflow.log_metric("training_loss", avg_loss, step=epoch)
        
        spark.stop()

        # --- 6. Log Model ---
        print(f"Logging model and registering new version as '{REGISTERED_MODEL_NAME}'...")
        mlflow.pytorch.log_model(
            pytorch_model=model,
            artifact_path="model",
            registered_model_name=REGISTERED_MODEL_NAME
        )

    print("\nPyTorch training and tracking complete.")

if __name__ == "__main__":
    main()