# Import necessary libraries, including pyarrow for iterative writing
import os
import requests
import zipfile
import io
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from sklearn.model_selection import train_test_split
from pathlib import Path

# --- (OPTIMIZATION) Helper function to reduce DataFrame memory ---
def optimize_datatypes(df: pd.DataFrame) -> pd.DataFrame:
    """Downcasts numerical columns to more memory-efficient types."""
    for col in df.select_dtypes(include=['float64', 'int64']).columns:
        if df[col].dtype == 'float64':
            df[col] = df[col].astype('float32')
        elif df[col].dtype == 'int64':
            df[col] = df[col].astype('int32')
    return df

# --- (OPTIMIZATION) Function to process a list of files and write to a single Parquet file ---
def create_parquet_from_files(file_list: list, output_path: Path):
    """
    Reads CSV files one by one, processes them using your original logic,
    and appends them to a single Parquet file on disk without loading all at once.
    """
    print(f"Processing {len(file_list)} files for -> {output_path}...")
    writer = None
    
    for i, file_path in enumerate(file_list):
        try:
            # Read one CSV into a temporary DataFrame
            temp_df = pd.read_csv(file_path)
            
            # 1. Optimize data types to reduce memory for this single file
            temp_df = optimize_datatypes(temp_df)
            
            # 2. Apply YOUR original logic to find device and label
            device_name = file_path.parts[-2]
            if 'attack' in device_name:
                device_name = file_path.parts[-3]
            temp_df['device'] = device_name
            temp_df['label'] = 'attack' if 'benign' not in file_path.name else 'benign'
            
            # Convert to a PyArrow Table for writing
            table = pa.Table.from_pandas(temp_df, preserve_index=False)
            
            # If this is the first file, create the writer with the table's schema
            if writer is None:
                writer = pq.ParquetWriter(output_path, table.schema)
            
            # Write the current file's data to the parquet file
            writer.write_table(table)

        except Exception as e:
            print(f"Could not process file {file_path}: {e}")
            
    if writer:
        writer.close()
    print(f"✅ Successfully created {output_path}")


# --- 1. Download and Unzip Dataset (Your Original Code) ---
print("Downloading N-BaIoT dataset...")
url = "https://archive.ics.uci.edu/static/public/442/detection+of+iot+botnet+attacks+n+baiot.zip"
raw_data_dir = "../data"

if not os.path.exists(raw_data_dir):
    try:
        r = requests.get(url, stream=True)
        r.raise_for_status()
        z = zipfile.ZipFile(io.BytesIO(r.content))
        z.extractall(raw_data_dir)
        print(f"Dataset downloaded and extracted to '{raw_data_dir}'.")
    except requests.exceptions.RequestException as e:
        print(f"Failed to download dataset: {e}")
else:
    print(f"Raw data directory '{raw_data_dir}' already exists. Skipping download.")


# --- 2. Get File List and Perform Stratified Split (OPTIMIZED) ---
data_dir = Path(raw_data_dir)
all_files = list(data_dir.rglob("*.csv"))

if not all_files:
    print("Error: No CSV files found. Please check the downloaded data.")
else:
    # Filter out the demonstration file using your logic
    filtered_files = [f for f in all_files if 'demonstrate_structure.csv' not in f.name]
    
    # Create labels for stratification based on filenames
    labels = ['attack' if 'benign' not in f.name else 'benign' for f in filtered_files]
    
    print(f"\nFound {len(filtered_files)} relevant CSV files.")
    print("Performing 80/20 split on the list of files (low memory)...")
    
    # Split the LIST OF FILES, not a loaded DataFrame
    train_files, val_files = train_test_split(
        filtered_files,
        test_size=0.2,
        random_state=42,
        stratify=labels
    )
    print(f"Files for training: {len(train_files)}")
    print(f"Files for validation: {len(val_files)}")


    # --- 3. Save Processed Data as Parquet Files (OPTIMIZED) ---
    output_base_path = Path("data/processed/iot_botnet_data")
    train_path = output_base_path / "train"
    val_path = output_base_path / "validation"
    train_path.mkdir(parents=True, exist_ok=True)
    val_path.mkdir(parents=True, exist_ok=True)

    # These paths now point to the final output files
    train_output_file = train_path / "train_data.parquet"
    val_output_file = val_path / "validation_data.parquet"
    
    # Process and write the training and validation sets iteratively
    create_parquet_from_files(train_files, train_output_file)
    create_parquet_from_files(val_files, val_output_file)

    print("\n🚀 Data preparation complete!")
    print("The processed data is now ready in the 'data/processed/iot_botnet_data' directory.")