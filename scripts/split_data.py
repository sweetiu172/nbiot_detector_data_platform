import os
import requests
import zipfile
import io
import pandas as pd
from sklearn.model_selection import train_test_split
from pathlib import Path

# --- 1. Download and Unzip Dataset ---
print("Downloading N-BaIoT dataset...")
# The dataset is hosted by the UCI Machine Learning Repository
url = "https://archive.ics.uci.edu/static/public/442/detection+of+iot+botnet+attacks+n+baiot.zip"
raw_data_dir = "../data"

# Check if data is already downloaded to avoid re-downloading
if not os.path.exists(raw_data_dir):
    try:
        r = requests.get(url, stream=True)
        r.raise_for_status()  # Raises an HTTPError for bad responses
        z = zipfile.ZipFile(io.BytesIO(r.content))
        z.extractall(raw_data_dir)
        print(f"Dataset downloaded and extracted to '{raw_data_dir}'.")
    except requests.exceptions.RequestException as e:
        print(f"Failed to download dataset: {e}")
        # Exit or handle the error appropriately
else:
    print(f"Raw data directory '{raw_data_dir}' already exists. Skipping download.")


# --- 2. Load and Combine Data from all CSVs ---
data_dir = Path(f"{raw_data_dir}")
all_files = list(data_dir.rglob("*.csv"))
df_list = []

# print("All files: ", all_files)

if not all_files:
    print("Error: No CSV files found. Please check the downloaded data.")
else:
    print(f"Found {len(all_files)} CSV files. Loading and combining...")
    for file in all_files:
        if 'demonstrate_structure.csv' in file.parts[-1]:
            print(file)
            continue
        try:
            # The device name is the parent directory's name
            device_name = file.parts[-2]
            if 'attack' in device_name:
                device_name = file.parts[-3]
            
            temp_df = pd.read_csv(file)
            temp_df['device'] = device_name
            
            # Create the binary label based on the filename
            temp_df['label'] = 'attack' if 'benign' not in file.name else 'benign'
            
            df_list.append(temp_df)
        except Exception as e:
            print(f"Could not process file {file}: {e}")

    # Concatenate all dataframes into a single one
    full_df = pd.concat(df_list, ignore_index=True)
    print("All files loaded and combined.")
    print(f"Full dataset shape: {full_df.shape}")
    print("\nLabel distribution in the full dataset:")
    print(full_df['label'].value_counts(normalize=True))


    # --- 3. Perform Stratified Train/Validation Split ---
    print("\nPerforming 80/20 train/validation split...")
    train_df, val_df = train_test_split(
        full_df,
        test_size=0.2,
        random_state=42,
        stratify=full_df['label']  # Crucial for maintaining class proportions
    )

    print(f"Training set shape: {train_df.shape}")
    print(f"Validation set shape: {val_df.shape}")

    print(train_df.head(5))


    # --- 4. Save Processed Data as Parquet Files ---
    output_base_path = Path("data/processed/iot_botnet_data")
    train_path = output_base_path / "train"
    val_path = output_base_path / "validation"

    # Create the necessary directory structure
    train_path.mkdir(parents=True, exist_ok=True)
    val_path.mkdir(parents=True, exist_ok=True)

    print(f"\nSaving training data to {train_path}/train_data.parquet...")
    train_df.to_parquet(train_path / "train_data.parquet", index=False)

    print(f"Saving validation data to {val_path}/validation_data.parquet...")
    val_df.to_parquet(val_path / "validation_data.parquet", index=False)

    print("\n✅ Data preparation complete!")
    print("The processed data is now ready in the 'data/processed/iot_botnet_data' directory.")