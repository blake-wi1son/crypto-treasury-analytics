import os
import requests as rq
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime

# Data Storage
RAW_DATA_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "raw"
)

def get_demo_key():
    # load environment variables from .env
    load_dotenv()
    # Get the API key from environment variables
    api_key = os.getenv("COINGECKO_DEMO_API_KEY")
    if api_key is None:
        raise ValueError("COINGECKO_DEMO_API_KEY not found in environment variables")
    return api_key

def save_as_csv(df, file_name, append = False):

    os.makedirs(RAW_DATA_DIR, exist_ok=True)
    filepath = os.path.join(RAW_DATA_DIR, file_name)
    
    # Append if file exists and append=True
    if append and os.path.exists(filepath):
        existing_df = pd.read_csv(filepath)
        df = pd.concat([existing_df, df], ignore_index=True)
        # Remove duplicates based on all columns
        df = df.drop_duplicates()
    
    df.to_csv(filepath, index=False)
    print(f"Saved: {filepath} ({len(df)} rows)")
    
    return filepath

# make api call
key = get_demo_key()
base_url = os.getenv("COINGECKO_DEMO_URL")

headers = {
            "accept": "application/json",
            "COINGECKO_DEMO_API_KEY": key
        }

params = {
            "vs_currency": "usd",
            "days": "1"
        }

endpoint = "/coins/ripple/market_chart"

url = f"{base_url}{endpoint}"
response = rq.get(url, headers=headers, params=params, timeout=30)
response.raise_for_status()
json_date = response.json()

# converting to a dataframe 
df = pd.DataFrame(json_date['prices'], columns=['timestamp', 'price'])
df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms') # convert timestamp record
    
# Add metadata columns
df["crypto_id"] = "xrp"
df["crypto_name"] = "Ripple"
df["extracted_at"] = datetime.now()
    
print(df.head())

#save dataframe to csv in data/raw

date_str = datetime.now().strftime("%Y%m%d")
filename = f"xrp_prices_{date_str}.csv"
save_as_csv(df, filename, append=False)
print("XRP extraction saved")