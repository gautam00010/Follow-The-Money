# AlphaWeave Quantitative Strategy Report

## Backtest Results
* **Total Trading Days Simulated:** 1256
* **Starting Capital:** $500000.00
* **Ending Capital:** $565222.17

## Performance by Ticker
| Ticker | Cumulative Return | Sharpe Ratio | Max Drawdown | Benchmark (B&H) | Recommendation |
| --- | --- | --- | --- | --- | --- |
| AAPL | -14.42% | -0.20 | 22.99% | 104.96% | HOLD |
| AMZN | -1.92% | 0.03 | 19.64% | 29.62% | HOLD |
| GOOGL | 16.81% | 0.38 | 11.05% | 168.20% | HOLD |
| MSFT | 16.37% | 0.36 | 18.57% | 51.66% | HOLD |
| NVDA | 52.67% | 0.65 | 15.24% | 1193.59% | BUY |
| TOTAL UNIVERSE | 13.04% | 0.30 | 12.86% | 309.61% | HOLD |

## Signal Methodology
This model systematically enters the market when alternative labor data (salary momentum) exhibits a Z-Score greater than 2.0 and exits to cash when momentum falls below -1.0.