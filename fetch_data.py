import os
import sys
import requests
import pandas as pd

# 1. Securely load API keys from GitHub Actions Environment
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
    """Fetches historical XLK ETF data from FMP and saves it to CSV."""
    if not FMP_API_KEY:
        raise ValueError("CRITICAL: FMP_API_KEY is missing from GitHub Secrets.")
    
    print("Fetching historical equity data from FMP Stable API...")
    url = f"https://financialmodelingprep.com/stable/historical-price-eod/full?symbol=XLK&apikey={FMP_API_KEY}"
    
    response = requests.get(url, timeout=30)
    
    # FAIL LOUDLY if the API blocks us
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
    """Fetches alternative labor market data from Adzuna and saves it to CSV."""
    if not ADZUNA_APP_ID or not ADZUNA_APP_KEY:
        raise ValueError("CRITICAL: ADZUNA API keys are missing from GitHub Secrets.")
        
    print("Fetching alternative labor data from Adzuna...")
    # Using the Adzuna 'histogram' endpoint to get time-series data for Machine Learning jobs
    url = f"https://api.adzuna.com/v1/api/jobs/us/histogram?app_id={ADZUNA_APP_ID}&app_key={ADZUNA_APP_KEY}&what=machine%20learning"
    
    response = requests.get(url, timeout=30)
    
    if response.status_code != 200:
        raise RuntimeError(f"Adzuna API failed with status {response.status_code}: {response.text}")
        
    data = response.json()
    
    if 'histogram' not in data:
        raise ValueError("Adzuna API response is missing the 'histogram' key.")
        
    # Adzuna returns {"histogram": {"2026-01": 1500, "2026-02": 1600}}
    # Convert this to a DataFrame with standard YYYY-MM-DD dates
    records = [{"date": f"{month}-01", "job_postings": count} for month, count in data["histogram"].items()]
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
        # This guarantees GitHub Actions will crash and turn red immediately if anything goes wrong
        print(f"PIPELINE CRITICAL ERROR: {str(e)}", file=sys.stderr)
        sys.exit(1)
