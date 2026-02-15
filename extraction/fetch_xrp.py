import os
import requests as rq
import pandas as pd
from datetime import datetime

from helper_functions import get_demo_key, save_as_csv

# Data Storage
RAW_DATA_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "raw"
)

# make api call
key = get_demo_key("COINGECKO_DEMO_API_KEY")
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
save_as_csv(df, filename, RAW_DATA_DIR, append=False)
print("XRP extraction saved")