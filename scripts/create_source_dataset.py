import os
import glob
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs
import argparse

# --- Configuration ---
# Point this to the local directory where you unzipped the N-BaIoT dataset
LOCAL_DATA_PATH = '../data' 
DEVICE_NAMES = [
    "Danmini_Doorbell", "Ecobee_Thermostat", "Ennio_Doorbell", 
    "Philips_B120N10_Baby_Monitor", "Provision_PT_737E_Security_Camera", 
    "Provision_PT_838_Security_Camera", "Samsung_SNH_1011_N_Webcam", 
    "SimpleHome_XCS7_1002_WHT_Security_Camera", "SimpleHome_XCS7_1003_WHT_Security_Camera"
]

# Destination in MinIO
OUTPUT_PATH = "processed/iot_botnet_data"

# MinIO Configuration (reads from environment variables)
S3_ENDPOINT_URL = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
S3_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minio_access_key")
S3_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minio_secret_key")


def create_dataset():
    """
    Reads all raw device CSVs, processes them, and writes a consolidated
    Parquet dataset to a MinIO S3 bucket.
    """
    s3 = s3fs.S3FileSystem(
        key=S3_ACCESS_KEY,
        secret=S3_SECRET_KEY,
        client_kwargs={'endpoint_url': S3_ENDPOINT_URL},
        use_ssl=False
    )

    if s3.exists(OUTPUT_PATH):
        print(f"Output path s3://{OUTPUT_PATH} already exists. Skipping creation.")
        return

    if not os.path.exists(LOCAL_DATA_PATH):
        print(f"ERROR: Local data path not found at '{LOCAL_DATA_PATH}'")
        print("Please download and extract the N-BaIoT dataset there first.")
        return

    all_device_dfs = []
    print("Starting data consolidation process...")

    for device in DEVICE_NAMES:
        print(f"  Processing device: {device}...")
        device_path = os.path.join(LOCAL_DATA_PATH, device)
        
        # 1. Load benign traffic
        benign_df = pd.read_csv(os.path.join(device_path, 'benign_traffic.csv'))
        benign_df['label'] = 'benign'
        
        # 2. Load all attack traffic
        attack_dfs = []
        attack_patterns = [
            os.path.join(device_path, 'gafgyt_attacks', '*.csv'),
            os.path.join(device_path, 'mirai_attacks', '*.csv')
        ]
        for pattern in attack_patterns:
            for attack_file in glob.glob(pattern):
                df = pd.read_csv(attack_file)
                attack_dfs.append(df)
        
        if not attack_dfs:
            print(f"    - Warning: No attack files found for {device}")
            continue

        attack_df_combined = pd.concat(attack_dfs, ignore_index=True)
        attack_df_combined['label'] = 'attack'
        
        # 3. Combine and add device name column
        device_df = pd.concat([benign_df, attack_df_combined], ignore_index=True)
        device_df['device_name'] = device
        all_device_dfs.append(device_df)

    print("Consolidating all device data...")
    full_df = pd.concat(all_device_dfs, ignore_index=True)
    print(f"Total records processed: {len(full_df):,}")
    print("Class distribution:\n", full_df['label'].value_counts())

    print(f"\nWriting consolidated data to s3://{OUTPUT_PATH} as Parquet...")
    table = pa.Table.from_pandas(full_df, preserve_index=False)
    pq.write_to_dataset(table, root_path=OUTPUT_PATH, filesystem=s3)
    
    print("\nData creation complete.")


if __name__ == "__main__":
    create_dataset()