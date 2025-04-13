import pandas as pd
import numpy as np
import swifter
import requests
import json
import os
import time
from typing import List, Dict, Set, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
from collections import deque
from threading import Lock
from rapidfuzz import process, fuzz

# Rate Limiter to respect OpenFIGI API limits (25 requests per 6 seconds)
class RateLimiter:
    def __init__(self, requests_limit: int, time_window: float):
        self.requests_limit = requests_limit  # 25 requests
        self.time_window = time_window  # 6 seconds
        self.requests = deque()
        self.lock = Lock()

    def __call__(self):
        with self.lock:
            now = time.time()
            # Remove requests older than the time window
            while self.requests and now - self.requests[0] > self.time_window:
                self.requests.popleft()
            # If we have room for more requests, proceed
            if len(self.requests) < self.requests_limit:
                self.requests.append(now)
                print(f"Request allowed at {now}, {len(self.requests)} requests in window")
                return
            # Otherwise, wait until the oldest request expires
            sleep_time = self.time_window - (now - self.requests[0])
            print(f"Rate limit hit, sleeping for {sleep_time:.2f} seconds")
            time.sleep(sleep_time)
            self.requests.append(time.time())
            print(f"Request allowed after sleep at {time.time()}, {len(self.requests)} requests in window")

rate_limiter = RateLimiter(requests_limit=25, time_window=6)

# Lock for safe cache updates
cache_lock = Lock()

# Load the Parquet file + Display Headings
news_article = pd.read_parquet('news_with_features_merged.parquet')
print("Headings:", news_article.columns.tolist())

# Change Sample Size Depending on the Data (.copy() for all data, .sample(n=1000) for 1000 random rows)
titlecolumn = news_article[['title', 'summary', 'NER']].copy()
print(f"Num of Data: {len(titlecolumn)}")

# Function to Extract company names and ticker-to-company mappings from NER, title, and summary
def extract_company_names(ner_data, title: str, summary: str) -> Tuple[List[str], List[Tuple[str, str]]]:
    companies = set()
    ticker_to_company = []  # List of (ticker, company_name) tuples
    
    # Step 1: Extract NER entities labeled as "ORG"
    ner_org_entities = []
    if ner_data is not None and len(ner_data) > 0:
        try:
            if isinstance(ner_data, str):
                entities = eval(ner_data.replace('array', '').replace('dtype=object', '').strip())
            elif isinstance(ner_data, (list, np.ndarray)):
                entities = ner_data
            else:
                return list(companies), ticker_to_company
            for entity in entities:
                if isinstance(entity, (list, tuple, np.ndarray)) and len(entity) >= 2:
                    text, label = entity[0], entity[1]
                    if label == "ORG":
                        companies.add(text)
                        ner_org_entities.append(text)
        except Exception as e:
            print(f"NER parsing error: {type(ner_data)} - {ner_data} - Error: {e}")

    # Step 2: Extract tickers from title and summary (e.g., "(NASDAQ:TSLA)")
    ticker_pattern = r'\b[A-Z]{1,5}\b(?=\s|$|\))|\((?:NASDAQ|NYSE|TSX|OTCQB):\s*([A-Z]{1,5})\)'
    text_sources = [(title, "title"), (summary, "summary")]
    
    for text, source in text_sources:
        matches = re.findall(ticker_pattern, text)
        for match in matches:
            if isinstance(match, tuple):
                ticker = next((t for t in match if t), None)
            else:
                ticker = match
            if ticker:
                # Do NOT add ticker to companies set to prevent it from appearing in company_names
                # Look for an "ORG" entity near the ticker in the text
                ticker_idx = text.find(ticker)
                if ticker_idx != -1:
                    # Extract a window of text around the ticker
                    start_idx = max(0, ticker_idx - 50)
                    end_idx = min(len(text), ticker_idx + len(ticker) + 50)
                    nearby_text = text[start_idx:end_idx]
                    # Find "ORG" entities in the window
                    nearby_orgs = [entity for entity in ner_org_entities if entity in nearby_text]
                    if nearby_orgs:
                        # Take the closest "ORG" entity to the ticker
                        closest_org = min(nearby_orgs, key=lambda name: abs(nearby_text.find(name) - nearby_text.find(ticker)))
                        ticker_to_company.append((ticker, closest_org))
                    else:
                        # If no "ORG" entity is found, use ticker as company name
                        ticker_to_company.append((ticker, ticker))

    # Step 3: Remove noise from companies set
    noise = {'GLOBE NEWSWIRE', 'Reuters', 'Business Insider'}
    companies = companies - noise
    return list(companies), ticker_to_company

# Extract company names and ticker mappings
titlecolumn[['company_names', 'ticker_mappings']] = titlecolumn.swifter.apply(
    lambda row: pd.Series(extract_company_names(row['NER'], row['title'], row['summary'])), axis=1
)

# Get unique company names
unique_companies = set()
for companies in titlecolumn['company_names']:
    unique_companies.update(companies)
unique_companies = list(unique_companies)

# Initialize Cache
cache_file = 'openfigi_ticker_cache.json'
if os.path.exists(cache_file):
    with open(cache_file, 'r') as f:
        company_to_ticker = json.load(f)
else:
    company_to_ticker = {}

# Clean company names for cache keys
def clean_company_names(name: str) -> str:
    if not name:
        return ""
    suffixes = r'\b(Corp|PLC|LTD|Inc|Incorporated|Corporation|Limited|Co\.?|Company)\b'
    return re.sub(suffixes, '', name, flags=re.IGNORECASE).strip()

# Update cache with ticker-to-company mappings
for _, row in titlecolumn.iterrows():
    for ticker, company in row['ticker_mappings']:
        cleaned_company = clean_company_names(company)
        if cleaned_company and ticker:
            company_to_ticker[cleaned_company] = ticker

# Save cache after adding ticker mappings
with open(cache_file, 'w') as f:
    json.dump(company_to_ticker, f)

# OpenFIGI API setup
api_key = "YOUR_API_KEY"
url = "https://api.openfigi.com/v3/mapping"
headers = {"X-OPENFIGI-APIKEY": api_key}

# Counter for total API requests
total_requests = 0

# Function to Get Ticker From OpenFIGI API with Retry Logic
def fetch_openfigi_tickers(companies: List[str], retries: int = 5, initial_wait: float = 6) -> Dict[str, str]:
    global total_requests
    if not companies:
        return {}
    
    for attempt in range(retries):
        rate_limiter()  # Enforce rate limit
        total_requests += 1
        print(f"Total requests made: {total_requests}")
        payload = [{"idType": "NAME", "idValue": c} for c in companies]
        try:
            response = requests.post(url, json=payload, headers=headers, timeout=10)
            if response.status_code == 200:
                results = response.json()
                matches = {}
                for i, result in enumerate(results):
                    if "data" in result and result["data"]:
                        cleaned_name = clean_company_names(companies[i])
                        matches[cleaned_name] = result["data"][0]["ticker"]
                return matches
            elif response.status_code == 429:
                wait_time = initial_wait * (2 ** attempt)  # Exponential backoff: 6s, 12s, 24s, etc.
                print(f"429 Too Many Requests (attempt {attempt+1}/{retries}), waiting {wait_time:.2f} seconds")
                time.sleep(wait_time)
            else:
                print(f"API error: {response.status_code} - {response.text}")
                wait_time = initial_wait * (2 ** attempt)
                print(f"Non-429 error, retrying after {wait_time:.2f} seconds")
                time.sleep(wait_time)
        except Exception as e:
            print(f"Request failed: {e}")
            wait_time = initial_wait * (2 ** attempt)
            print(f"Exception occurred, retrying after {wait_time:.2f} seconds")
            time.sleep(wait_time)
    
    # If all retries fail, return an empty dictionary instead of None values
    print(f"Failed to fetch tickers. Skipping this batch.")
    return {}

# Check cache first, then batch process unmatched companies
cleaned_unique_companies = [clean_company_names(c) for c in unique_companies if c]
unmatched = [c for c in cleaned_unique_companies if c not in company_to_ticker]
batch_size = 100  # Fits within 100 jobs per request

def parallel_fetch(batch):
    matches = fetch_openfigi_tickers(batch, retries=5, initial_wait=6)
    # Only update cache if matches were found
    if matches:
        with cache_lock:
            company_to_ticker.update(matches)
            with open(cache_file, 'w') as f:
                json.dump(company_to_ticker, f)
    else:
        print(f"No matches found for batch, cache not updated.")
    return matches

batches = [unmatched[i:i + batch_size] for i in range(0, len(unmatched), batch_size)]
# Reduced max_workers to 1 to ensure sequential processing and avoid rate limit issues
with ThreadPoolExecutor(max_workers=1) as executor:
    future_to_batch = {executor.submit(parallel_fetch, batch): batch for batch in batches}
    for future in as_completed(future_to_batch):
        future.result()

# Optimized Function to Map Company Name with Ticker
def map_to_ticker(companies: List[str]) -> List[str]:
    if not companies:
        return []
    tickers = set()
    cached_names = list(company_to_ticker.keys())
    
    # Step 1: Direct lookups and ticker checks
    fuzzy_candidates = []
    fuzzy_indices = []
    for i, company in enumerate(companies):
        cleaned_company = clean_company_names(company)
        if not cleaned_company:
            continue
        # Direct cache lookup with case variations
        ticker = (company_to_ticker.get(cleaned_company) or
                  company_to_ticker.get(cleaned_company.title()) or
                  company_to_ticker.get(cleaned_company.upper()))
        if ticker:
            tickers.add(ticker)
            continue
        # Check if it's a ticker already
        if cleaned_company.isupper() and len(cleaned_company) <= 5 and re.match(r'^[A-Z]{1,5}$', cleaned_company):
            tickers.add(cleaned_company)
            continue
        # Skip fuzzy matching for unlikely cases (too short or too long)
        if len(cleaned_company) < 4 or len(cleaned_company) > 50:
            continue
        fuzzy_candidates.append(cleaned_company)
        fuzzy_indices.append(i)
    
    # Step 2: Batch fuzzy matching with rapidfuzz
    fuzzy_threshold = 90  # Increased from 80 to 90
    if fuzzy_candidates and cached_names:
        # Use process.extract to batch-process fuzzy matching
        matches = process.extract(fuzzy_candidates, cached_names, scorer=fuzz.partial_ratio, score_cutoff=fuzzy_threshold, limit=1)
        for (query, score, choice), idx in zip(matches, fuzzy_indices):
            ticker = company_to_ticker.get(choice)
            if ticker:
                tickers.add(ticker)
    
    # Step 3: Substring matching with early exit
    for i, company in enumerate(companies):
        cleaned_company = clean_company_names(company)
        if not cleaned_company:
            continue
        # Skip if already matched via direct lookup, ticker check, or fuzzy matching
        if (company_to_ticker.get(cleaned_company) or
            company_to_ticker.get(cleaned_company.title()) or
            company_to_ticker.get(cleaned_company.upper()) or
            (cleaned_company.isupper() and len(cleaned_company) <= 5 and re.match(r'^[A-Z]{1,5}$', cleaned_company))):
            continue
        # Perform substring matching
        for cached_name in cached_names:
            if len(cached_name) >= 4 and cached_name.lower() in cleaned_company.lower():
                ticker = company_to_ticker.get(cached_name)
                if ticker:
                    tickers.add(ticker)
                    break  # Early exit after first match
    
    return list(tickers)

# Map the extracted company names to tickers
titlecolumn['stock_tickers'] = titlecolumn['company_names'].swifter.apply(map_to_ticker)

# Drop temporary ticker_mappings column
titlecolumn = titlecolumn.drop(columns=['ticker_mappings'])

# Post-processing: Remove leading/trailing commas in company_names
titlecolumn['company_names'] = titlecolumn['company_names'].apply(
    lambda x: ', '.join(x) if x else ''
)
titlecolumn['stock_tickers'] = titlecolumn['stock_tickers'].apply(
    lambda x: ', '.join(x) if x else ''
)
titlecolumn['NER'] = titlecolumn['NER'].where(titlecolumn['NER'].isna(), titlecolumn['NER'].astype(str)).fillna('[]')

# Save to CSV
titlecolumn.to_csv('title_column.csv', index=True, index_label='newsID')