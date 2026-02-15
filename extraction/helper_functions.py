import os
from dotenv import load_dotenv
import pandas as pd

def get_demo_key(key_name):
    # load environment variables from .env
    load_dotenv()
    # Get the API key from environment variables
    api_key = os.getenv(key_name)
    if api_key is None:
        raise ValueError(f"{key_name} not found in environment variables")
    return api_key


def save_as_csv(df, file_name, save_loc, append = False):

    os.makedirs(save_loc, exist_ok=True)
    filepath = os.path.join(save_loc, file_name)
    
    # Append if file exists and append=True
    if append and os.path.exists(filepath):
        existing_df = pd.read_csv(filepath)
        df = pd.concat([existing_df, df], ignore_index=True)
        # Remove duplicates based on all columns
        df = df.drop_duplicates()
    
    df.to_csv(filepath, index=False)
    print(f"Saved: {filepath} ({len(df)} rows)")
    
    return filepath