import os
import sys
import pandas as pd

# Define directories
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DATA_DIR = os.path.join(BASE_DIR, "data", "raw")
PROCESSED_DATA_DIR = os.path.join(BASE_DIR, "data", "processed")

def ensure_directory():
    if not os.path.exists(PROCESSED_DATA_DIR):
        os.makedirs(PROCESSED_DATA_DIR)

def _rolling_zscore(series, window, min_periods=5):
    mean_series = series.rolling(window, min_periods=min_periods).mean()
    std_series = series.rolling(window, min_periods=min_periods).std()
    safe_std = std_series.replace(0, 1) # Prevent divide by zero
    zscore = (series - mean_series) / safe_std
    return zscore.fillna(0.0)

def build_signals():
    equity_path = os.path.join(RAW_DATA_DIR, "equity_prices.csv")
    jobs_path = os.path.join(RAW_DATA_DIR, "job_postings.csv")

    print("Loading raw data...")
    equity_df = pd.read_csv(equity_path)
    jobs_df = pd.read_csv(jobs_path)

    # Convert to proper datetime
    equity_df['date'] = pd.to_datetime(equity_df['date'])
    jobs_df['date'] = pd.to_datetime(jobs_df['date'])

    # THE MASTER QUANT FIX: Calendar Alignment
    # 1. Outer merge keeps BOTH trading days and the 1st of the month
    merged = pd.merge(equity_df, jobs_df, on="date", how="outer")
    
    # 2. Sort chronologically
    merged = merged.sort_values("date")
    
    # 3. Forward-fill the job postings to fill the gaps between the 1st of each month
    merged["job_postings"] = merged["job_postings"].ffill()
    
    # 4. Now remove weekends/holidays by dropping rows where the stock market was closed
    merged = merged.dropna(subset=["close"])
    
    # 5. Drop the beginning of the timeline where we didn't have job data yet
    merged = merged.dropna(subset=["job_postings"])

    if merged.empty:
        raise ValueError("Merged dataset is empty after alignment. Check date ranges.")

    print("Calculating quantitative signals...")
    merged["job_zscore"] = _rolling_zscore(merged["job_postings"], window=30, min_periods=5)

    final_df = merged[['date', 'close', 'job_zscore']]

    out_path = os.path.join(PROCESSED_DATA_DIR, "signals.csv")
    final_df.to_csv(out_path, index=False)
    print(f"SUCCESS: Signal file created with {len(final_df)} days of trading data at {out_path}")

if __name__ == "__main__":
    try:
        ensure_directory()
        build_signals()
    except Exception as e:
        print(f"SIGNAL FUSION ERROR: {str(e)}", file=sys.stderr)
        sys.exit(1)
