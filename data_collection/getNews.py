import pandas as pd
import os
import requests
import json
import datetime
import time

os.environ['alphavantage_api_key'] = 'Your_API_Key'
alphavantage_api_key = os.getenv('alphavantage_api_key')

BASE_URL = "https://www.alphavantage.co/query"

start_date = datetime.datetime.strptime("2022-03-01", "%Y-%m-%d")
end_date = datetime.datetime.strptime("2025-02-17", "%Y-%m-%d")

current_date = end_date

while current_date >= start_date:
  # Intervals: 00:00-09:59, 10:00-12:59, 13:00-15:59, 16:00-19:59, 20:00-23:59
  intervals = [(0, 9), (10, 12), (13, 15), (16, 19), (20, 23)]
  
  for idx, (start_hour, end_hour) in enumerate(intervals):
    time_from_dt = current_date.replace(hour=start_hour, minute=0, second=0)
    time_to_dt = current_date.replace(hour=end_hour, minute=59, second=59)
    time_from = time_from_dt.strftime("%Y%m%dT%H%M")
    time_to = time_to_dt.strftime("%Y%m%dT%H%M")
    params = {
      "function": "NEWS_SENTIMENT",
      "time_from": time_from,
      "time_to": time_to,
      "apikey": alphavantage_api_key,
      "limit": 1000  # Max allowed
    }

    try:
      response = requests.get(BASE_URL, params=params)
      response.raise_for_status()  # Raise HTTP errors
      data = response.json()

      if "feed" in data:
        print(f"Fetched {len(data['feed'])} articles for {current_date.date()} interval {idx}")
        with open(f"datalake_news_articles/news_{current_date.date()}_{idx}.json", "w") as f:
          json.dump(data, f)
      else:
        print(f"No data for {current_date.date()} interval {idx}: {data}")
    except Exception as e:
      print(f"Request failed for {current_date.date()} interval {idx}: {e}")
  
  current_date -= datetime.timedelta(days=1)  # Move to previous day
  # time.sleep(1) # Avoid rate limits