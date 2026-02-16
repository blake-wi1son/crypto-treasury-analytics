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

DATE_STR = datetime.now().strftime("%Y-%m-%d")

def fetch_fed_data(series_id=None, start_date=DATE_STR):

    key = get_demo_key("FRED_API_KEY")
    base_url = "https://api.stlouisfed.org"
    endpoint = "/fred/series/observations"
    url = f"{base_url}{endpoint}"

    params = {
                "api_key": key,
                "series_id": series_id,
                "file_type": "json",
                "observation_start": start_date,
            }

    response = rq.get(url, params=params, timeout=30)
    response.raise_for_status()
    json_data = response.json()['observations']

    # converting to a dataframe 
    df = pd.DataFrame(json_data, columns=['date', f'value'])
    df = df.rename(columns={"value": f"{series_id}_value"})

    return df

dff_data = fetch_fed_data(series_id="DFF")
dgs2_data = fetch_fed_data(series_id="DGS2")
dgs10_data = fetch_fed_data(series_id="DGS10")
    
# Combine into one DataFrame
combined_df = pd.merge(dff_data, dgs2_data, on="date", how="outer")
combined_df = pd.merge(combined_df, dgs10_data, on="date", how="outer")

print(combined_df.head())

#save dataframe to csv in data/raw
filename = f"fed_data_{DATE_STR}.csv"
save_as_csv(combined_df, filename, RAW_DATA_DIR, append=False)
print("FED data extraction saved")
