import pandas as pd
import os
import requests
import json
import datetime
import time

os.environ['alphavantage_api_key'] = 'YOUR_API_KEY'
alphavantage_api_key = os.getenv('alphavantage_api_key')

BASE_URL = "https://www.alphavantage.co/query"

params = {
  "function": "LISTING_STATUS",
  "state": "active", # or delisted
  "apikey": alphavantage_api_key
}
  
try:
  response = requests.get(BASE_URL, params=params)
  response.raise_for_status()  # Raise HTTP errors
except Exception as e:
  print(f"{e}")

from io import StringIO
df = pd.read_csv(StringIO(response.text))

df.to_parquet("data_warehouse/listed_stocks.parquet")