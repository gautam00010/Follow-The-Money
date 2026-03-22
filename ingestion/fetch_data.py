from __future__ import annotations

import os
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterable

import pandas as pd
import requests


FMP_BASE_URL = "https://financialmodelingprep.com/api/v3/historical-price-full"
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

    url = f"{FMP_BASE_URL}/{symbol}"
    response = requests.get(url, params={"apikey": api_key}, timeout=30)
    if response.status_code != 200:
        raise DataIngestionError(
            f"FMP request failed with status {response.status_code}: {response.text}"
        )

    payload = response.json()
    historical = payload.get("historical")
    if not historical:
        raise DataIngestionError("FMP response missing 'historical' data.")

    df = pd.DataFrame(historical)
    if "date" not in df.columns or "close" not in df.columns:
        raise DataIngestionError("FMP response does not include required fields.")

    df["date"] = pd.to_datetime(df["date"], utc=True)
    df = df.sort_values("date")

    output_path = Path(output_path)
    _ensure_directory(output_path)
    df.to_csv(output_path, index=False)
    return output_path


def _convert_epoch_to_date(value: int | float) -> date:
    # Adzuna returns epoch seconds; protect against millisecond epochs.
    if value > MILLISECOND_EPOCH_VALUE:
        if value > MICROSECOND_EPOCH_VALUE:
            raise DataIngestionError(
                "Adzuna timestamp appears to be in microseconds/nanoseconds; unsupported format."
            )
        value = value / 1000.0
    return datetime.fromtimestamp(value, tz=timezone.utc).date()


def fetch_job_postings(
    keywords: Iterable[str] | None = None,
    country: str = "us",
    output_path: Path | str = Path("data/raw/job_postings.csv"),
) -> Path:
    """
    Fetch historical job posting counts for selected growth keywords from Adzuna.

    Parameters
    ----------
    keywords : Iterable[str] | None
        Keywords that proxy for corporate expansion (e.g., machine learning hires).
    country : str
        Two-letter country code supported by Adzuna.
    output_path : Path | str
        Destination CSV path for the raw histogram.

    Returns
    -------
    Path
        Path to the written CSV file.
    """
    if keywords is None:
        keywords = DEFAULT_EXPANSION_KEYWORDS

    app_id = os.getenv("ADZUNA_APP_ID")
    app_key = os.getenv("ADZUNA_APP_KEY")
    if not app_id or not app_key:
        raise DataIngestionError("ADZUNA_APP_ID and ADZUNA_APP_KEY must be set.")

    rows: list[dict] = []
    for keyword in keywords:
        url = ADZUNA_HISTORY_URL_TEMPLATE.format(country=country)
        response = requests.get(
            url,
            params={"app_id": app_id, "app_key": app_key, "what": keyword},
            timeout=30,
        )
        if response.status_code != 200:
            raise DataIngestionError(
                f"Adzuna request failed for '{keyword}' with status "
                f"{response.status_code}: {response.text}"
            )

        payload = response.json()
        # Adzuna history endpoints sometimes expose the histogram under different keys
        # depending on country/API version.
        histogram = payload.get("histogram") or payload.get("results")
        if not histogram:
            raise DataIngestionError(f"Adzuna response missing histogram for {keyword}.")

        for entry in histogram:
            epoch = entry.get("time") or entry.get("date")
            count = entry.get("count") or entry.get("value") or entry.get("total")
            if epoch is None or count is None:
                continue
            rows.append(
                {
                    "date": _convert_epoch_to_date(epoch),
                    "keyword": keyword,
                    "count": float(count),
                }
            )

    if not rows:
        raise DataIngestionError("No job posting observations were collected.")

    df = pd.DataFrame(rows)
    df_grouped = (
        df.groupby("date", as_index=False)["count"]
        .sum()
        .rename(columns={"count": "job_postings"})
    )

    output_path = Path(output_path)
    _ensure_directory(output_path)
    df_grouped.to_csv(output_path, index=False)
    return output_path


if __name__ == "__main__":
    equity_path = fetch_equity_data()
    jobs_path = fetch_job_postings()
    print(f"Wrote equity data to {equity_path}")
    print(f"Wrote job data to {jobs_path}")
