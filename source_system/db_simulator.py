import time
import random
import os
from sqlalchemy import create_engine, text
import pandas as pd

DB_URI = os.getenv('DB_URI', 'postgresql://postgres:mysecretpassword@localhost:5432/iot_db')
TABLE_NAME = 'iot_events' # A new, simpler table for events
DEVICES = ["Danmini_Doorbell", "Ecobee_Thermostat", "Ennio_Doorbell", "Philips_B120N_Baby_Monitor", "Provision_PT_737E_Security_Camera"]

def create_table_if_not_exists(engine):
    """Creates the source event table."""
    try:
        with engine.connect() as connection:
            connection.execute(text(f"""
                CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                    event_id SERIAL PRIMARY KEY,
                    device_name VARCHAR(100),
                    ground_truth_label VARCHAR(50)
                );
                ALTER TABLE {TABLE_NAME} REPLICA IDENTITY FULL;
            """))
            connection.commit()
        print(f"Table '{TABLE_NAME}' is ready.")
    except Exception as e:
        print(f"Error creating table: {e}")

def main():
    """Periodically inserts new 'thin' events into the source database."""
    engine = create_engine(DB_URI)
    create_table_if_not_exists(engine)
    
    print(f"Continuously inserting 'thin' events into table '{TABLE_NAME}'...")
    while True:
        try:
            event_type = 'attack' if random.random() < 0.1 else 'benign'
            device = random.choice(DEVICES)
            
            # Create a simple DataFrame for the event
            event_df = pd.DataFrame([{'device_name': device, 'ground_truth_label': event_type}])
            
            # Insert the new event
            event_df.to_sql(TABLE_NAME, engine, if_exists='append', index=False)
            print(f"Inserted 1 '{event_type}' event for device '{device}'.")
            
            time.sleep(2)
        except Exception as e:
            print(f"An error occurred: {e}")
            time.sleep(5)

if __name__ == '__main__':
    main()