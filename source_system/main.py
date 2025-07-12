# main.py
import pandas as pd
import boto3
import argparse
import os
import shutil
from generator import generate_data

# --- Configuration ---
BENIGN_SUMMARY_PATH = 'feature_distribution_summary_benign.csv'
ATTACK_SUMMARY_PATH = 'feature_distribution_summary_attack.csv'
DEVICES_TO_SIMULATE =  ["Danmini_Doorbell", "Ecobee_Thermostat", "Ennio_Doorbell", "Philips_B120N10_Baby_Monitor", "Provision_PT_737E_Security_Camera", "Provision_PT_838_Security_Camera", "Samsung_SNH_1011_N_Webcam", "SimpleHome_XCS7_1002_WHT_Security_Camera", "SimpleHome_XCS7_1003_WHT_Security_Camera"]

# Simulate a mix of 95% benign and 5% attack records
RECORDS_PER_DEVICE_TOTAL = 1000
BENIGN_RECORDS_COUNT = int(RECORDS_PER_DEVICE_TOTAL * 0.95)
ATTACK_RECORDS_COUNT = RECORDS_PER_DEVICE_TOTAL - BENIGN_RECORDS_COUNT

LOCAL_OUTPUT_DIR = './generated_data'

# --- MinIO Configuration (fetched from environment variables) ---
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'localhost:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minio_access_key')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minio_secret_key')
LANDING_BUCKET = os.getenv('LANDING_BUCKET', 'landing')

def upload_directory_to_minio(local_path: str, bucket_name: str, s3_client):
    """Uploads a directory and its contents to a MinIO bucket."""
    for root, _, files in os.walk(local_path):
        for file in files:
            local_file = os.path.join(root, file)
            relative_path = os.path.relpath(local_file, local_path)
            s3_key = relative_path.replace("\\", "/")
            
            print(f"Uploading {local_file} to s3://{bucket_name}/{s3_key}")
            s3_client.upload_file(local_file, bucket_name, s3_key)

def main(date_str: str):
    """
    Generates and uploads partitioned data for a specific day.
    """
    print(f"Starting data generation for date: {date_str}")
    
    if os.path.exists(LOCAL_OUTPUT_DIR):
        shutil.rmtree(LOCAL_OUTPUT_DIR)
        
    # Load the summary statistics for both types
    benign_summary_df = pd.read_csv(BENIGN_SUMMARY_PATH, index_col=0).rename_axis('feature').reset_index()
    attack_summary_df = pd.read_csv(ATTACK_SUMMARY_PATH, index_col=0).rename_axis('feature').reset_index()

    for device in DEVICES_TO_SIMULATE:
        print(f"Generating data for device: {device}")
        
        # Generate benign and attack data separately
        benign_df = generate_data(benign_summary_df, BENIGN_RECORDS_COUNT, 'benign')
        attack_df = generate_data(attack_summary_df, ATTACK_RECORDS_COUNT, 'attack') # Generic 'attack' label
        
        # Combine, shuffle, and save
        combined_df = pd.concat([benign_df, attack_df]).sample(frac=1).reset_index(drop=True)
        
        partition_path = os.path.join(LOCAL_OUTPUT_DIR, f"dt={date_str}", f"device_name={device}")
        os.makedirs(partition_path, exist_ok=True)
        
        output_file = os.path.join(partition_path, 'data.csv')
        combined_df.to_csv(output_file, index=False)
        print(f"Saved combined data locally to {output_file}")

    # Connect to MinIO and upload
    s3_client = boto3.client(
        's3',
        endpoint_url=f'http://{MINIO_ENDPOINT}',
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )
    
    print("\nStarting upload to MinIO...")
    upload_directory_to_minio(LOCAL_OUTPUT_DIR, LANDING_BUCKET, s3_client)
    print("\nSimulation complete. Data uploaded to MinIO landing bucket.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Source system simulator.")
    parser.add_argument("--date", required=True, type=str, help="Date in YYYY-MM-DD format.")
    args = parser.parse_args()
    main(args.date)