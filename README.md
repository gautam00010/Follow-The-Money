# AlphaWeave MVP

AlphaWeave is a lightweight alternative data signal engine that fuses labor market momentum with equity prices to detect macro regime shifts.

## Pipeline overview
1. **Ingestion (`ingestion/fetch_data.py`)**  
   - Downloads Technology Select Sector SPDR (XLK) price history from Financial Modeling Prep.  
   - Pulls historical job posting counts for expansionary keywords from the Adzuna API.
2. **Signal Fusion (`signals/fuse_signals.py`)**  
   - Joins labor and price series, computes 30-day hiring momentum, and derives a rolling z-score to flag statistically significant hiring spikes.
3. **Backtesting (`backtest_engine/engine.cpp`)**  
   - Runs a simple systematic strategy: long XLK when hiring z-score > 2.0, exit/reduce when < -1.0.  
   - Reports cumulative return, max drawdown, and Sharpe Ratio to `REPORT.md`.

## Running locally
```bash
python -m pip install -r requirements.txt
python ingestion/fetch_data.py
python signals/fuse_signals.py
cd backtest_engine && make && ./engine
```

Set environment variables before running:
- `FMP_API_KEY`
- `ADZUNA_APP_ID`
- `ADZUNA_APP_KEY`
