from __future__ import annotations

from pathlib import Path

import pandas as pd


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Required input not found: {path}")
    return pd.read_csv(path, parse_dates=["date"])


def _rolling_zscore(
    series: pd.Series,
    window: int,
    min_periods: int = 5,
    rolling_mean: pd.Series | None = None,
) -> pd.Series:
    mean_series = (
        rolling_mean if rolling_mean is not None else series.rolling(window, min_periods=min_periods).mean()
    )
    std_series = series.rolling(window, min_periods=min_periods).std()
    safe_std = std_series.replace(0, pd.NA)
    zscore = (series - mean_series) / safe_std
    return zscore.fillna(0.0)


def build_signals(
    equity_path: Path | str = Path("data/raw/equity_prices.csv"),
    jobs_path: Path | str = Path("data/raw/job_postings.csv"),
    output_path: Path | str = Path("data/processed/signals.csv"),
    ma_window: int = 30,
    zscore_window: int = 30,
) -> Path:
    """
    Merge equity and labor data to produce a trading signal file.

    The z-score flags statistically significant hiring spikes that often
    precede growth regimes in technology equities.
    """
    equity_df = _read_csv(Path(equity_path))
    jobs_df = _read_csv(Path(jobs_path))

    merged = pd.merge(equity_df, jobs_df, on="date", how="inner")
    if merged.empty:
        raise ValueError("Merged dataset is empty; check source overlaps.")

    merged = merged.sort_values("date")
    job_ma = merged["job_postings"].rolling(ma_window, min_periods=5).mean()
    merged[f"job_ma_{ma_window}d"] = job_ma
    rolling_mean_for_zscore = job_ma if ma_window == zscore_window else None
    merged["job_zscore"] = _rolling_zscore(
        merged["job_postings"],
        window=zscore_window,
        min_periods=5,
        rolling_mean=rolling_mean_for_zscore,
    )

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(output_path, index=False)
    return output_path


if __name__ == "__main__":
    output = build_signals()
    print(f"Wrote fused signals to {output}")
