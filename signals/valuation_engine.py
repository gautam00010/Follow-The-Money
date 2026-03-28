import os
import sys
from typing import List, Dict, Any

import pandas as pd
import requests

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROCESSED_DATA_DIR = os.path.join(BASE_DIR, "data", "processed")
FMP_API_KEY = os.environ.get("FMP_API_KEY")
TECH_UNIVERSE = ["AAPL", "MSFT", "NVDA", "GOOGL", "AMZN"]


class ValuationAnalyst:
    """
    Institutional-grade valuation workflow that blends fundamentals with alternative signals.
    """

    def __init__(self, api_key: str, universe: List[str]) -> None:
        """
        Initialize the analyst with API credentials and a coverage universe.

        Parameters
        ----------
        api_key : str
            Financial Modeling Prep API key.
        universe : List[str]
            List of ticker symbols to evaluate.
        """
        if not api_key:
            raise ValueError("CRITICAL: FMP_API_KEY is missing from environment.")
        self.api_key = api_key
        self.universe = universe
        self.session = requests.Session()

    def _ensure_processed_dir(self) -> None:
        """
        Ensure the processed data directory exists.
        """
        if not os.path.exists(PROCESSED_DATA_DIR):
            os.makedirs(PROCESSED_DATA_DIR)

    def fetch_key_metrics(self) -> pd.DataFrame:
        """
        Fetch key fundamental metrics (PE ratio, Debt/Equity) for the coverage universe.

        Returns
        -------
        pd.DataFrame
            DataFrame with columns ['symbol', 'peRatio', 'debtToEquity'].
        """
        records: List[Dict[str, Any]] = []
        for symbol in self.universe:
            url = f"https://financialmodelingprep.com/api/v3/key-metrics/{symbol}"
            params = {"apikey": self.api_key, "limit": 1}
            try:
                response = self.session.get(url, params=params, timeout=30)
                if response.status_code != 200:
                    raise RuntimeError(f"Status {response.status_code}: {response.text}")
                payload = response.json()
                if not payload or not isinstance(payload, list):
                    raise ValueError(f"Unexpected response for {symbol}: {payload}")
                latest = payload[0]
                record = {
                    "symbol": symbol,
                    "peRatio": latest.get("peRatio"),
                    "debtToEquity": latest.get("debtToEquity"),
                }
                if record["peRatio"] is None or record["debtToEquity"] is None:
                    raise ValueError(f"Missing metrics for {symbol}: {latest}")
                records.append(record)
            except Exception as exc:
                print(f"WARNING: Failed to fetch metrics for {symbol} ({type(exc).__name__}): {exc}", file=sys.stderr)
                continue
        if not records:
            raise RuntimeError("No key metrics could be fetched for the universe.")
        return pd.DataFrame(records)

    def load_latest_salary_signal(self) -> float:
        """
        Load the latest salary z-score from processed signals.

        Returns
        -------
        float
            The most recent job_zscore value from signals.csv.
        """
        signals_path = os.path.join(PROCESSED_DATA_DIR, "signals.csv")
        if not os.path.exists(signals_path):
            raise FileNotFoundError(f"signals.csv not found at {signals_path}")
        signals_df = pd.read_csv(signals_path)
        if "job_zscore" not in signals_df.columns:
            raise KeyError("signals.csv is missing 'job_zscore' column.")
        latest_row = signals_df.tail(1).iloc[0]
        return float(latest_row["job_zscore"])

    def compute_quality_scores(self, fundamentals: pd.DataFrame, salary_z: float) -> pd.DataFrame:
        """
        Compute Quality Score combining relative valuation and salary momentum.

        Parameters
        ----------
        fundamentals : pd.DataFrame
            DataFrame containing 'symbol' and 'peRatio'.
        salary_z : float
            Latest salary z-score signal.

        Returns
        -------
        pd.DataFrame
            DataFrame with quality_score added.
        """
        pe_series = fundamentals["peRatio"].astype(float)
        pe_mean = pe_series.mean()
        pe_std = pe_series.std()
        if pe_std == 0:
            pe_std = 1.0
        fundamentals = fundamentals.copy()
        fundamentals["pe_relative"] = -((pe_series - pe_mean) / pe_std)
        fundamentals["salary_zscore"] = salary_z
        fundamentals["quality_score"] = fundamentals["pe_relative"] + fundamentals["salary_zscore"]
        return fundamentals.drop(columns=["pe_relative"])

    def build_research_summary(self) -> str:
        """
        Orchestrate metric fetch, signal merge, quality scoring, and export.

        Returns
        -------
        str
            Path to the generated research summary CSV.
        """
        self._ensure_processed_dir()
        fundamentals = self.fetch_key_metrics()
        salary_z = self.load_latest_salary_signal()
        enriched = self.compute_quality_scores(fundamentals, salary_z)
        enriched = enriched[["symbol", "peRatio", "debtToEquity", "salary_zscore", "quality_score"]]
        enriched = enriched.sort_values(by="quality_score", ascending=False)
        out_path = os.path.join(PROCESSED_DATA_DIR, "research_summary.csv")
        enriched.to_csv(out_path, index=False)
        print(f"SUCCESS: Saved research summary for {len(enriched)} tickers to {out_path}")
        return out_path


def main() -> None:
    """
    Entry point for running the valuation engine standalone.
    """
    try:
        analyst = ValuationAnalyst(api_key=FMP_API_KEY, universe=TECH_UNIVERSE)
        analyst.build_research_summary()
    except Exception as exc:
        print(f"VALUATION ENGINE ERROR: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
