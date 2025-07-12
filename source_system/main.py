import pandas as pd
import numpy as np
import time
import random
import yaml
from sqlalchemy import create_engine
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
    table_name = pg_config['table_name']

    # Handle potential NaN values in the stats file by replacing them with 0.
    # This prevents the script from generating NaN, which becomes NULL in the database.
    print("Handling potential missing values in feature statistics...")
    feature_names = stats_df.index.tolist()
    mean_values = stats_df['mean'].fillna(0)
    std_values = stats_df['std'].fillna(0)

    try:
        print(f"Connecting to PostgreSQL at {pg_config['db_url'].split('@')[-1]}...")
        engine = create_engine(pg_config['db_url'])
        with engine.connect() as connection:
            print("Database connection successful.")
    except OperationalError as e:
        print(f"FATAL: Could not connect to PostgreSQL. Check your db_url. Error: {e}")
        return

    print(f"Starting data simulation. Inserting 1 row every ~{sim_config['interval_seconds']} seconds.")
    print(f"Writing to table: {table_name}. Press Ctrl+C to stop.")

    devices = ['Danmini_Doorbell', 'Ecobee_Thermostat', 'Samsung_SNH_1011_N_Webcam']
    
    while True:
        try:
            # 1. Generate a single row of data using the cleaned-up stats
            features = np.random.normal(loc=mean_values, scale=std_values)
            features[features < 0] = 0

            new_row_dict = dict(zip(feature_names, features))
            new_row_dict['device_name'] = random.choice(devices)
            
            # 2. Convert the single row to a pandas DataFrame
            row_df = pd.DataFrame([new_row_dict])

            # 3. Use pandas' `to_sql` function to safely insert the data
            row_df.to_sql(
                table_name,
                engine,
                if_exists='append',
                index=False
            )

            print(f"[{time.ctime()}] Inserted 1 row for device '{new_row_dict['device_name']}'.")

            # 4. Wait for the next cycle
            time.sleep(sim_config['interval_seconds'])

        except KeyboardInterrupt:
            print("\n--- Simulation stopped by user. ---")
            break
        except Exception as e:
            print(f"\nAn error occurred during the simulation loop: {e}")
            time.sleep(5)

if __name__ == "__main__":
    try:
        distribution_stats = pd.read_csv("feature_distribution_summary.csv", index_col=0)
    except FileNotFoundError:
        print("FATAL: `feature_distribution_summary.csv` not found. Please run the analysis script first.")
        exit()
        
    config = load_config()
    run_source_simulator(config, distribution_stats)