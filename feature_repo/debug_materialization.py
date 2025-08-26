import pandas as pd
from feast import FeatureStore

print("--- Starting Materialization Debug Script (v2) ---")

try:
    # Connect to your feature store
    store = FeatureStore(repo_path=".")
    
    # --- 1. Define a list of devices you KNOW exist in your data ---
    # We will check if Feast can find features for these specific devices.
    # Replace these with actual device names from your dataset.
    entity_df = pd.DataFrame.from_dict({
        "device_name": [
            "Danmini_Doorbell", 
            "Ecobee_Thermostat"
        ],
        "event_timestamp": [
            pd.to_datetime("2025-08-21 10:00:00Z"),
            pd.to_datetime("2025-08-21 10:00:00Z"),
        ]
    })
    
    print(f"\nAttempting to pull features for {len(entity_df)} known devices...")
    
    # --- 2. Use the correct public API to get historical features ---
    # This uses the same underlying logic as the 'materialize' command.
    historical_features_df = store.get_historical_features(
        entity_df=entity_df,
        features=[
            "nbiot_features:MI_dir_L5_mean",
            "nbiot_features:H_L5_mean",
        ],
    ).to_df()
    
    print("\n--- Result of Feast's Data Retrieval Logic ---")
    print(f"Number of rows returned: {len(historical_features_df)}")
    print("DataFrame head:")
    print(historical_features_df.head())
    print("---------------------------------------------")

    if not historical_features_df.empty and len(historical_features_df) > 0:
        print("\n✅ SUCCESS: Feast's internal query is correctly retrieving data.")
    else:
        print("\n❌ FAILED: Feast's internal query is returning an empty DataFrame. This is the root cause.")

except Exception as e:
    print(f"\nAn error occurred: {e}")