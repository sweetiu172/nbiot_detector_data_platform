import os
import pandas as pd
import random
import yaml
from datetime import datetime, timedelta
from tqdm import tqdm

# --- Configuration ---
DATE_RANGE_DAYS = 90  # Simulate data as if it arrived over the last 90 days

def load_config(config_path='config.yaml'):
    """Loads the configuration from a YAML file."""
    print(f"Loading configuration from {config_path}...")
    try:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        print(f"FATAL: Configuration file not found at '{config_path}'")
        exit()

def load_and_process_all_csvs(source_path):
    """
    Loads all CSVs from the source path, adds metadata, and returns a single DataFrame.
    """
    all_dfs = []
    print(f"Scanning for all CSV files in '{source_path}'...")
    files_to_process = [os.path.join(dirpath, f)
                        for dirpath, _, filenames in os.walk(source_path)
                        for f in filenames if f.endswith('.csv')]

    if not files_to_process:
        raise FileNotFoundError(f"No CSV files found in the source directory: {source_path}")

    print(f"Found {len(files_to_process)} CSV files. Starting processing...")
    for file_path in tqdm(files_to_process, desc="Processing CSV files"):
        try:
            df = pd.read_csv(file_path)
            
            # --- THIS IS THE CORRECTED LOGIC ---
            # Create a reliable relative path from the source directory
            relative_path = os.path.relpath(file_path, source_path)
            path_parts = relative_path.split(os.sep)
            
            # Now path_parts[0] is guaranteed to be the device name folder
            # We check if the path has at least a device folder and a file.
            if len(path_parts) >= 2:
                df['device_name'] = path_parts[0]

                if "benign_traffic.csv" in path_parts[-1]:
                    df['attack_type'] = 'benign'
                # Path structure for attacks is Device/Attack_Family/Attack_Name.csv
                elif len(path_parts) >= 3:
                    attack_family = path_parts[1]
                    attack_name = os.path.splitext(path_parts[2])[0]
                    df['attack_type'] = f"{attack_family}_{attack_name}"
                else:
                    df['attack_type'] = 'unknown'
                
                all_dfs.append(df)
            else:
                print(f"\nWarning: Skipping file with unexpected path structure: {file_path}")
            # --- END OF CORRECTION ---

        except Exception as e:
            print(f"\nWarning: Could not process file {file_path}. Error: {e}")

    print("Consolidating all data into a single DataFrame...")
    return pd.concat(all_dfs, ignore_index=True)

def add_randomized_dates(df, num_days):
    """Adds a randomized event_date to each row in the DataFrame."""
    print(f"Adding randomized event dates over the last {num_days} days...")
    end_date = datetime.now()
    start_date = end_date - timedelta(days=num_days)
    total_rows = len(df)
    random_seconds = [random.randint(0, int((end_date - start_date).total_seconds())) for _ in range(total_rows)]
    df['event_date'] = [start_date + timedelta(seconds=sec) for sec in random_seconds]
    return df

def write_partitioned_parquet_to_minio(df, config):
    """
    Writes the DataFrame directly to MinIO as a partitioned Parquet dataset.
    """
    datalake_config = config['datalake']
    
    endpoint_url = datalake_config['endpoint']
    if not endpoint_url.startswith(('http://', 'https://')):
        endpoint_url = 'http://' + endpoint_url
    
    # Using the path from your latest config.yaml
    s3_path = f"s3://{datalake_config['bucket_name']}/{datalake_config['processed_path']}"
    print(f"Preparing to write partitioned Parquet data to MinIO at: {s3_path}")

    if df.empty:
        print("DataFrame is empty, nothing to write.")
        return

    df['dt'] = pd.to_datetime(df['event_date']).dt.strftime('%Y-%m-%d')
    df = df.drop(columns=['event_date'])

    storage_options = {
        'key': datalake_config['access_key'],
        'secret': datalake_config['secret_key'],
        'client_kwargs': {
            'endpoint_url': endpoint_url
        }
    }
    
    partitioning_columns = ['device_name', 'dt']
    print(f"Partitioning data by: {partitioning_columns}")

    try:
        df.to_parquet(
            s3_path,
            partition_cols=partitioning_columns,
            index=False,
            engine='pyarrow',
            storage_options=storage_options
        )
        print(f"\n✅ Successfully wrote partitioned Parquet dataset to MinIO.")
    except Exception as e:
        print(f"\nFATAL: An error occurred while writing to MinIO. Error: {e}")
        print("Please check your MinIO connection details and ensure the service is running.")

if __name__ == "__main__":
    try:
        config = load_config('config.yaml')
        master_df = load_and_process_all_csvs(config['source_data']['base_path'])
        if not master_df.empty:
            master_df_with_dates = add_randomized_dates(master_df, DATE_RANGE_DAYS)
            write_partitioned_parquet_to_minio(master_df_with_dates, config)
        else:
            print("No data was processed, exiting.")
    except Exception as e:
        print(f"\nAn unexpected error occurred in the main process: {e}")