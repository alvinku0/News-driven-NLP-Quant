import json
import glob
import pandas as pd
from datetime import datetime

# List all json files in the news_data directory
json_files = glob.glob("datalake_news_articles/news_*.json")

# Initialize a list to store the article data from all files
all_articles = []

for file in json_files:
  try:
    with open(file, "r") as f:
      data = json.load(f)
    # Check if there's a feed and extend our list with articles
    if "feed" in data:
      all_articles.extend(data["feed"])
  except Exception as e:
    print(f"Error reading {file}: {e}")


df = pd.DataFrame(all_articles)

# parse the time_published
df['time_published'] = pd.to_datetime(df['time_published'])

# sort the articles by time_published
df = df.sort_values(by='time_published')

# assign an newsID to each article
df['newsID'] = range(1, 1+len(df))

# set newsID as the index
df.set_index('newsID', inplace=True)

df.to_parquet("data_warehouse/news_articles.parquet")