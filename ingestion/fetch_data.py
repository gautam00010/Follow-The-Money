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
TECH_UNIVERSE = ["AAPL", "MSFT", "NVDA", "GOOGL", "AMZN"]

def ensure_directory():
    """Create the data/raw/ directory if it doesn't exist."""
    if not os.path.exists(RAW_DATA_DIR):
        os.makedirs(RAW_DATA_DIR)

def fetch_equity_data():
    """Fetches historical data for the tech universe from FMP and saves it to CSV."""
    if not FMP_API_KEY:
        raise ValueError("CRITICAL: FMP_API_KEY is missing from GitHub Secrets.")

    ensure_directory()
    all_frames = []
    failed_symbols = []

    for symbol in TECH_UNIVERSE:
        print(f"Fetching historical equity data for {symbol} from FMP Stable API...")
        url = f"https://financialmodelingprep.com/stable/historical-price-eod/full?symbol={symbol}&apikey={FMP_API_KEY}"

        try:
            response = requests.get(url, timeout=30)

            if response.status_code != 200:
                raise RuntimeError(f"FMP API failed with status {response.status_code}: {response.text}")

            data = response.json()

            if not data or not isinstance(data, list):
                raise ValueError(
                    f"FMP API returned unexpected structure for {symbol}. Expected a flat list, got: {str(data)[:100]}"
                )

            df = pd.DataFrame(data)

            if "date" not in df.columns or "close" not in df.columns or "volume" not in df.columns:
                raise KeyError(f"FMP data for {symbol} is missing 'date', 'close', or 'volume' columns.")

            subset = df[["date", "close", "volume"]].assign(symbol=symbol)
            all_frames.append(subset)

        except Exception as e:
            failed_symbols.append(symbol)
            print(f"WARNING: Failed to fetch data for {symbol}: {str(e)}", file=sys.stderr)
            continue

    if not all_frames:
        raise RuntimeError("FMP API fetch failed for all symbols; no data to save.")

    combined = pd.concat(all_frames, ignore_index=True)
    combined["date"] = pd.to_datetime(combined["date"], errors="coerce")
    invalid_mask = combined["date"].isna()
    if invalid_mask.any():
        dropped_rows = invalid_mask.sum()
        symbols_with_invalid_dates = ", ".join(sorted(combined.loc[invalid_mask, "symbol"].unique()))
        print(
            f"WARNING: Dropped {dropped_rows} rows with invalid dates during parsing. Affected symbols: {symbols_with_invalid_dates}.",
            file=sys.stderr,
        )
    combined = combined[~invalid_mask].copy()
    combined = combined.sort_values(by=["date", "symbol"])
    # Serialize to ISO 8601 strings for a stable, tidy CSV artifact
    combined["date"] = combined["date"].dt.strftime("%Y-%m-%d")
    combined = combined[["date", "symbol", "close", "volume"]]

    csv_path = os.path.join(RAW_DATA_DIR, "universe_prices.csv")
    combined.to_csv(csv_path, index=False)
    success_count = len(TECH_UNIVERSE) - len(failed_symbols)
    print(f"SUCCESS: Saved {len(combined)} rows to {csv_path} from {success_count} tickers")

    if failed_symbols:
        print(f"Completed with warnings. Failed tickers: {', '.join(failed_symbols)}", file=sys.stderr)

    return csv_path

def fetch_job_postings():
    """Fetches historical salary trends for ML jobs as our alternative data signal."""
    if not ADZUNA_APP_ID or not ADZUNA_APP_KEY:
        raise ValueError("CRITICAL: ADZUNA API keys are missing from GitHub Secrets.")

    ensure_directory()
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
        fetch_equity_data()
        fetch_job_postings()
        print("Data Ingestion Step Complete.")
    except Exception as e:
        print(f"PIPELINE CRITICAL ERROR: {str(e)}", file=sys.stderr)
        sys.exit(1)
