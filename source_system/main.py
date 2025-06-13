import pandas as pd
import numpy as np
import time
import random
import yaml
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

def load_config(config_path='config.yaml'):
    """Loads the configuration from a YAML file."""
    print(f"Loading configuration from {config_path}...")
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def run_source_simulator(config, stats_df):
    """
    Simulates a source system by continuously generating and inserting data
    into a PostgreSQL database.
    """
    pg_config = config['postgres']
    sim_config = config['simulation']

    # Extract feature statistics for data generation
    feature_names = stats_df.index.tolist()
    mean_values = stats_df['mean']
    std_values = stats_df['std']

    try:
        print(f"Connecting to PostgreSQL at {pg_config['db_url'].split('@')[-1]}...")
        engine = create_engine(pg_config['db_url'])
        with engine.connect() as connection:
            print("Database connection successful.")
    except OperationalError as e:
        print(f"FATAL: Could not connect to PostgreSQL. Check your db_url. Error: {e}")
        return

    print(f"Starting data simulation. Inserting 1 row every ~{sim_config['interval_seconds']} seconds.")
    print(f"Writing to table: {pg_config['table_name']}. Press Ctrl+C to stop.")

    devices = ['Danmini_Doorbell', 'Ecobee_Thermostat', 'Samsung_SNH_1011_N_Webcam']
    attacks = ['benign', 'gafgyt_combo', 'gafgyt_udp', 'mirai_syn']
    
    while True:
        try:
            # 1. Generate a single row of data
            features = np.random.normal(loc=mean_values, scale=std_values)
            features[features < 0] = 0  # Ensure non-negative

            # Create a dictionary for the new row
            new_row = dict(zip(feature_names, features))
            new_row['device_name'] = random.choice(devices)
            new_row['attack_type'] = random.choice(attacks)
            new_row['event_timestamp'] = pd.Timestamp.now(tz='UTC')

            # 2. Insert the new row into PostgreSQL
            with engine.connect() as connection:
                # We build the insert statement dynamically to handle all feature columns
                columns = ", ".join([f'"{col}"' for col in new_row.keys()])
                values = ", ".join([f":{col}" for col in new_row.keys()])
                
                # Using text() is important for passing parameters safely
                stmt = text(f"INSERT INTO {pg_config['table_name']} ({columns}) VALUES ({values})")
                
                connection.execute(stmt, new_row)
                connection.commit() # Commit the transaction

            print(f"[{time.ctime()}] Inserted 1 row for device '{new_row['device_name']}'.")

            # 3. Wait for the next cycle
            time.sleep(sim_config['interval_seconds'])

        except KeyboardInterrupt:
            print("\n--- Simulation stopped by user. ---")
            break
        except Exception as e:
            print(f"\nAn error occurred during the simulation loop: {e}")
            time.sleep(5) # Wait before retrying

if __name__ == "__main__":
    # Load feature distributions from the original project file
    try:
        distribution_stats = pd.read_csv("feature_distribution_summary.csv", index_col=0)
    except FileNotFoundError:
        print("FATAL: `feature_distribution_summary.csv` not found. Please run the analysis script from the original project first.")
        exit()
        
    config = load_config()
    run_source_simulator(config, distribution_stats)