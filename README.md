# Follow-The-Money

A "living" GitHub repository that automatically tracks and lists insider stock purchases (Form 4, code `P`) over **$50,000** using only GitHub Actions.

## How it works

1. A Python script (`scripts/fetch_insider_purchases.py`) pulls the latest Form 4 filings from the SEC current filings feed, extracts purchase (`P`) transactions above $50K, and writes them into `daily_report.md`.
2. A scheduled GitHub Actions workflow runs every day at **08:00 UTC** (and can also be triggered manually) to refresh the report and commit changes back to the repository.

## Running locally

```bash
python -m pip install -r requirements.txt
SEC_USER_AGENT="Your Name contact@example.com" python scripts/fetch_insider_purchases.py
```

Notes:
- Set `SEC_USER_AGENT` to a contact string that complies with SEC fair-use guidance.
- Optional environment variables:
  - `INSIDER_PURCHASE_THRESHOLD` (default: `50000`)
  - `INSIDER_FEED_COUNT` (default: `100`)

## Workflow details

- Workflow file: `.github/workflows/daily-insider-report.yml`
- Schedule: `0 8 * * *` (08:00 UTC daily)
- Output: `daily_report.md` in the repository root
- Requires `contents: write` permission to commit the refreshed report. The workflow uses [`stefanzweifel/git-auto-commit-action`](https://github.com/stefanzweifel/git-auto-commit-action) to commit changes when present.
