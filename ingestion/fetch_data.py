import os
import sys
import requests
import pandas as pd

# 1. Securely load API keys
FMP_API_KEY = os.environ.get("FMP_API_KEY")
ADZUNA_APP_ID = os.environ.get("ADZUNA_APP_ID")
ADZUNA_APP_KEY = os.environ.get("ADZUNA_APP_KEY")

# 2. Define exactly where the CSVs must be saved
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DATA_DIR = os.path.join(BASE_DIR, "data", "raw")

def ensure_directory():
    """Create the data/raw/ directory if it doesn't exist."""
    if not os.path.exists(RAW_DATA_DIR):
        os.makedirs(RAW_DATA_DIR)

def fetch_equity_data():
    """Fetches historical AAPL data from FMP and saves it to CSV."""
    if not FMP_API_KEY:
        raise ValueError("CRITICAL: FMP_API_KEY is missing from GitHub Secrets.")
    
    print("Fetching historical equity data for AAPL from FMP Stable API...")
    url = f"https://financialmodelingprep.com/stable/historical-price-eod/full?symbol=AAPL&apikey={FMP_API_KEY}"
    
    response = requests.get(url, timeout=30)
    
    if response.status_code != 200:
        raise RuntimeError(f"FMP API failed with status {response.status_code}: {response.text}")
    
    data = response.json()
    
    if not data or not isinstance(data, list):
        raise ValueError(f"FMP API returned unexpected structure. Expected a flat list, got: {str(data)[:100]}")
        
    df = pd.DataFrame(data)
    
    if 'date' not in df.columns or 'close' not in df.columns:
        raise KeyError("FMP data is missing 'date' or 'close' columns.")
        
    csv_path = os.path.join(RAW_DATA_DIR, "equity_prices.csv")
    df.to_csv(csv_path, index=False)
    print(f"SUCCESS: Saved {len(df)} rows to {csv_path}")
    return csv_path

def fetch_job_postings():
    """Fetches historical salary trends for ML jobs as our alternative data signal."""
    if not ADZUNA_APP_ID or not ADZUNA_APP_KEY:
        raise ValueError("CRITICAL: ADZUNA API keys are missing from GitHub Secrets.")
        
    print("Fetching alternative labor data (Salary History) from Adzuna...")
    # THE FIX: Switch to the 'history' endpoint to get real YYYY-MM dates
    url = f"https://api.adzuna.com/v1/api/jobs/us/history?app_id={ADZUNA_APP_ID}&app_key={ADZUNA_APP_KEY}&what=machine%20learning"
    
    response = requests.get(url, timeout=30)
    
    if response.status_code != 200:
        raise RuntimeError(f"Adzuna API failed with status {response.status_code}: {response.text}")
        
    data = response.json()
    
    if 'month' not in data:
        raise ValueError("Adzuna API response is missing the 'month' key.")
        
    # data["month"] looks like {"2025-10": 145000, "2025-11": 146000}
    # We map the salary values into the 'job_postings' column so the C++ engine doesn't break
    records = [{"date": f"{month_str}-01", "job_postings": value} for month_str, value in data["month"].items()]
    df = pd.DataFrame(records)
    
    if df.empty:
        raise ValueError("Adzuna API returned an empty dataset.")
        
    csv_path = os.path.join(RAW_DATA_DIR, "job_postings.csv")
    df.to_csv(csv_path, index=False)
    print(f"SUCCESS: Saved {len(df)} rows to {csv_path}")
    return csv_path

if __name__ == "__main__":
    try:
        ensure_directory()
        fetch_equity_data()
        fetch_job_postings()
        print("Data Ingestion Step Complete.")
    except Exception as e:
        print(f"PIPELINE CRITICAL ERROR: {str(e)}", file=sys.stderr)
        sys.exit(1)
