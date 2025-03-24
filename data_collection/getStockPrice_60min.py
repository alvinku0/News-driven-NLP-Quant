# Not using asynchronous programming due to API rate limit

import pandas as pd
import os
import requests
import json
import datetime
import time
from io import StringIO
from dateutil.relativedelta import relativedelta
import datetime

# Load the list of tickers
df = pd.read_parquet('data_warehouse/listed_stocks.parquet')
symbol_ipoDate_pairs = tuple(zip(df['symbol'], df['ipoDate']))

os.environ['alphavantage_api_key'] = 'YOUR_API_KEY'
alphavantage_api_key = os.getenv('alphavantage_api_key')

BASE_URL = "https://www.alphavantage.co/query"

start_date = datetime.date(2018, 1, 1)
end_date = datetime.date(2025, 1, 1)

for ticker, ipoDate in symbol_ipoDate_pairs:

  dfs = []

  current_date = end_date
  ipoDate = datetime.datetime.strptime(ipoDate, "%Y-%m-%d").date()
  stop_date = max( ipoDate - relativedelta(months=1) , start_date )
  
  while current_date >= stop_date:
    month = current_date.strftime("%Y-%m")
    
    params = {
      "function": "TIME_SERIES_INTRADAY",
      "symbol": ticker,
      "interval": "60min",
      "adjusted": "true",
      "extended_hours": "true",
      "month": month,
      "outputsize": "full",
      "datatype": "csv",
      "apikey": alphavantage_api_key
    }

    try:
      response = requests.get(BASE_URL, params=params)
      response.raise_for_status()

      if response.text[0] != "{":
        df_month = pd.read_csv(StringIO(response.text))
        dfs.append(df_month)
        print(f"Retrieved {len(df_month)} rows of data. Month: {month}. Ticker: {ticker}")
      else:
        print(f"Error retrieving data for {month}, {ticker}: {response.text}")

    except Exception as e:
      print(f"Error retrieving data for {month}, {ticker}: {e}")
      time.sleep(2)
      continue

    current_date -= relativedelta(months=1)
  
  # concat all dataframes from the same ticker 
  # if dfs is empty, skip to the next ticker
  if dfs:

    df_concat = pd.concat(dfs, ignore_index=True)
    df_concat['ticker'] = ticker

    # set timestamp as index
    df_concat['timestamp'] = pd.to_datetime(df_concat['timestamp'])
    df_concat.set_index('timestamp', inplace=True)

    # sort by timestamp
    df.sort_index(inplace=True)

    # save as parquet
    df_concat.to_parquet(f"datalake_stock_price_60min/{ticker}.parquet")
