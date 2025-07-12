# scripts/train.py
import mlflow
import mlflow.pytorch
import torch
import torch.nn as nn
from petastorm import make_reader
from petastorm.pytorch import DataLoader

def main():
    """Main training script for the PyTorch MLP model."""
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
    mlflow.set_experiment("iot_botnet_detection_pytorch_batch")
    print("MLflow setup complete.")

    with mlflow.start_run() as run:
        # --- 3. Hyperparameters & Model ---
        LEARNING_RATE = 0.001
        EPOCHS = 5
        INPUT_SIZE = 115
        device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        print(f"Using device: {device}")
        model = MLPDetector(input_size=INPUT_SIZE, hidden_size_1=128, hidden_size_2=64, output_size=1, dropout_rate=0.3).to(device)
        criterion = nn.BCEWithLogitsLoss()
        optimizer = torch.optim.Adam(model.parameters(), lr=LEARNING_RATE)

        mlflow.log_params({"learning_rate": LEARNING_RATE, "epochs": EPOCHS, "batch_size": 256})
        print("Model and hyperparameters initialized.")

        # --- 4. Training Loop ---
        print(f"Beginning training for {EPOCHS} epochs...")
        feature_store_path = "s3a://processed/training_ready_features"
        storage_options = {"key": "minioadmin", "secret": "minioadmin", "endpoint_url": "http://minio:9000", "use_ssl": False}

        with DataLoader(make_reader(dataset_url=feature_store_path, reader_pool_type='thread', storage_options=storage_options)) as train_loader:
            for epoch in range(EPOCHS):
                model.train()
                total_loss = 0
                num_batches = 0
                for batch in train_loader:
                    features = batch['features_flat'].float().to(device)
                    labels = batch['indexedLabel'].float().unsqueeze(1).to(device)
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

        # --- 5. Log Model ---
        print("Logging model to MLflow...")
        mlflow.pytorch.log_model(model, "model")

    print("\nPyTorch batch training and tracking complete.")

if __name__ == "__main__":
    main()