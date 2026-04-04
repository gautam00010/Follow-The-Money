# AlphaWeave Quantitative Strategy Report

## Backtest Results
* **Total Trading Days Simulated:** 1256
* **Starting Capital:** $500000.00
* **Ending Capital:** $553559.11

## Performance by Ticker
| Ticker | Cumulative Return | Sharpe Ratio | Max Drawdown | Benchmark (B&H) | Recommendation |
| --- | --- | --- | --- | --- | --- |
| AAPL | -10.46% | -0.30 | 14.07% | 103.27% | HOLD |
| AMZN | -0.91% | 0.02 | 19.64% | 30.02% | HOLD |
| GOOGL | 13.75% | 0.39 | 11.05% | 166.58% | HOLD |
| MSFT | 4.55% | 0.16 | 18.57% | 49.94% | HOLD |
| NVDA | 50.67% | 0.95 | 11.80% | 1167.98% | BUY |
| TOTAL UNIVERSE | 10.71% | 0.43 | 10.59% | 303.56% | HOLD |

## Signal Methodology
This model systematically enters the market when alternative labor data (salary momentum) exhibits a Z-Score greater than 2.0 and exits to cash when momentum falls below -1.0.