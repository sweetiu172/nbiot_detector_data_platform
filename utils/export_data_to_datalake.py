import os
import yaml
from minio import Minio
from minio.error import S3Error
import sys

def load_config(config_path='config.yaml'):
    """Loads the configuration from a YAML file."""
    print(f"Loading configuration from {config_path}...")
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
            print("Configuration loaded successfully.")
            return config
    except FileNotFoundError:
        print(f"FATAL: Configuration file not found at '{config_path}'")
        sys.exit(1)
    except Exception as e:
        print(f"FATAL: Error reading configuration file: {e}")
        sys.exit(1)

def upload_files_to_minio(config):
    """
    Connects to MinIO and uploads a limited number of raw CSV files as
    specified in the configuration.
    """
    # Load configuration details
    source_config = config['source_data']
    datalake_config = config['datalake']
    
    source_path = source_config['base_path']
    # num_files_to_upload = source_config.get('num_files') # Use .get() for safety

    endpoint = datalake_config['endpoint']
    access_key = datalake_config['access_key']
    secret_key = datalake_config['secret_key']
    bucket_name = datalake_config['bucket_name']
    upload_folder = datalake_config['folder_name'] # Using folder_name as specified

    print(f"\nAttempting to connect to MinIO at {endpoint}...")
    
    try:
        # Initialize MinIO client
        client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=False  # Set to False for localhost HTTP
        )

        # Ensure the bucket exists
        if not client.bucket_exists(bucket_name):
            print(f"Bucket '{bucket_name}' not found. Creating it...")
            client.make_bucket(bucket_name)
        else:
            print(f"Bucket '{bucket_name}' already exists.")

        # First, collect all eligible CSV files from the source directory
        all_csv_files = []
        if not os.path.isdir(source_path):
            print(f"FATAL: Source data directory not found at '{source_path}'")
            sys.exit(1)
            
        for dirpath, _, filenames in os.walk(source_path):
            for filename in filenames:
                if filename.endswith('.csv'):
                    all_csv_files.append(os.path.join(dirpath, filename))
        
        print(f"Found a total of {len(all_csv_files)} CSV files in the source directory.")


        # Loop through the selected files and upload them
        for i, local_file_path in enumerate(all_csv_files):
            # Create a relative path to replicate the structure in MinIO
            relative_path = os.path.relpath(local_file_path, source_path)
            # Define the object name in the bucket using the specified 'folder_name'
            object_name = os.path.join(upload_folder, relative_path).replace("\\", "/")

            # Upload the file
            print(f"  Uploading file {i+1}/{len(all_csv_files)}: '{local_file_path}' to '{bucket_name}/{object_name}'...")
            client.fput_object(bucket_name, object_name, local_file_path)
        
        print(f"\n✅ Upload of {len(all_csv_files)} files complete!")

    except S3Error as exc:
        print(f"FATAL: An error occurred with MinIO: {exc}")
        sys.exit(1)
    except Exception as e:
        print(f"FATAL: An unexpected error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    config = load_config()
    upload_files_to_minio(config)