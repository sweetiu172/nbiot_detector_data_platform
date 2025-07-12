# generator.py
import pandas as pd
import numpy as np

def generate_data(summary_df: pd.DataFrame, num_records: int, label: str) -> pd.DataFrame:
    """
    Generates a DataFrame with randomized data based on a given statistical profile.
    """
    generated_data = {}
    for _, row in summary_df.iterrows():
        feature_name = row['feature']
        mean = row['mean']
        std = row['std']
        
        # Generate data using a normal distribution and ensure it's non-negative
        generated_features = np.random.normal(loc=mean, scale=std, size=num_records)
        generated_data[feature_name] = np.maximum(0, generated_features)
        
    generated_df = pd.DataFrame(generated_data)
    
    # Assign the specified label to all records
    generated_df['label'] = label
    
    return generated_df