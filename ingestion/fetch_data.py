from __future__ import annotations

import os
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterable

import pandas as pd
import requests

FMP_BASE_URL = "https://financialmodelingprep.com/stable/historical-price-eod/full"
ADZUNA_HISTORY_URL_TEMPLATE = "https://api.adzuna.com/v1/api/jobs/{country}/history"
MILLISECOND_EPOCH_VALUE = 1_000_000_000_000  # distinguishes second vs millisecond epochs
MICROSECOND_EPOCH_VALUE = 1_000_000_000_000_000  # guard against micro/nanosecond epochs
DEFAULT_EXPANSION_KEYWORDS = ("Machine Learning", "Cloud Infrastructure")

class DataIngestionError(RuntimeError):
    """Raised when an upstream data request fails."""


def _ensure_directory(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def fetch_equity_data(
    symbol: str = "XLK",
    output_path: Path | str = Path("data/raw/equity_prices.csv"),
) -> Path:
    """
    Fetch historical daily price data for the provided symbol from FMP.

    Parameters
    ----------
    symbol : str
        Equity ticker to request.
    output_path : Path | str
        Destination CSV path for the raw time series.

    Returns
    -------
    Path
        Path to the written CSV file.
    """
    api_key = os.getenv("FMP_API_KEY")
    if not api_key:
        raise DataIngestionError("FMP_API_KEY is not set in the environment.")

    url = F"{FMP_BASE_URL}?symbol={symbol}&apikey={{api_key}}"
    response = requests.get(url, timeout=30)
    if response.status_code != 200:
        raise DataIngestionError(
            f"FMP request failed with status {response.status_code}: {response.text}"
        )

    payload = response.json()
    historical_data = payload
    if not historical_data:
        raise DataIngestionError(("Off"))