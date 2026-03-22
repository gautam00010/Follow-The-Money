#!/usr/bin/env python3
from __future__ import annotations

import logging
import os
import re
import csv
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Iterable, List, Optional
from urllib.parse import urljoin
from xml.etree import ElementTree as ET

import requests
from requests import HTTPError, RequestException

SEC_FEED_URL = (
    "https://www.sec.gov/cgi-bin/browse-edgar?"
    "action=getcurrent&CIK=&type=4&company=&dateb=&owner=only&start=0&count={count}&output=atom"
)
USER_AGENT = os.environ.get(
    "SEC_USER_AGENT",
    "Gautam Jha mgstudiooo0@gmail.com",
)
OUTPUT_FILE = Path(__file__).resolve().parent.parent / "daily_report.md"
CSV_FILE = Path(__file__).resolve().parent.parent / "historical_insider_buys.csv"
MINIMUM_VALUE = Decimal(os.environ.get("INSIDER_PURCHASE_THRESHOLD", "50000"))
FEED_COUNT = int(os.environ.get("INSIDER_FEED_COUNT", "100"))
REQUEST_TIMEOUT = 30


@dataclass
class InsiderPurchase:
    date: str
    issuer_name: str
    ticker: str
    insider: str
    shares: Decimal
    price: Decimal
    value: Decimal
    filing_url: str


def strip_namespaces(root: ET.Element) -> None:
    """Remove namespaces in-place to simplify downstream parsing."""
    for element in root.iter():
        if "}" in element.tag:
            element.tag = element.tag.split("}", 1)[1]


def to_decimal(raw: Optional[str]) -> Optional[Decimal]:
    if raw is None:
        return None
    try:
        return Decimal(str(raw).strip())
    except (InvalidOperation, ValueError):
        return None


def fetch_text(url: str) -> str:
    api_key = os.environ.get("SCRAPERAPI_KEY")
    
    # If the key exists, route the request through the residential proxy
    if api_key:
        target_url = f"http://api.scraperapi.com?api_key={api_key}&url={url}"
    else:
        target_url = url
        
    # Proxies can take a few extra seconds to route the traffic
    response = requests.get(target_url, timeout=60)
    response.raise_for_status()
    return response.text


def parse_feed_entries(feed_xml: str) -> Iterable[dict]:
    ns = {"atom": "http://www.w3.org/2005/Atom"}
    root = ET.fromstring(feed_xml)
    entries = []
    for entry in root.findall("atom:entry", ns):
        link_el = entry.find("atom:link", ns)
        link = link_el.attrib.get("href") if link_el is not None else None
        if not link:
            continue
        match = re.search(r"/data/(?P<cik>\d+)/(?P<accession>\d+)/", link)
        if not match:
            continue
        entries.append(
            {
                "link": link,
                "title": entry.findtext("atom:title", default="").strip(),
                "updated": entry.findtext("atom:updated", default=""),
                "cik": match.group("cik"),
                "accession": match.group("accession"),
            }
        )
    return entries


def find_xml_document_href(index_html: str) -> Optional[str]:
    candidates = re.findall(r'href="([^"]+\.xml)"', index_html, flags=re.IGNORECASE)
    if not candidates:
        return None

    priority_order = (
        "form4",
        "f345",
        "doc4",
        "primary_doc",
        "ownership",
    )
    for keyword in priority_order:
        for candidate in candidates:
            if keyword in candidate.lower():
                return candidate
    return candidates[0]


def parse_purchase_transactions(xml_text: str, minimum_value: Decimal = MINIMUM_VALUE) -> List[InsiderPurchase]:
    root = ET.fromstring(xml_text)
    strip_namespaces(root)

    issuer_name = root.findtext(".//issuerName", default="").strip()
    ticker = root.findtext(".//issuerTradingSymbol", default="").strip()
    insider = root.findtext(".//reportingOwner//rptOwnerName", default="").strip()

    purchases: List[InsiderPurchase] = []
    for txn in root.findall(".//nonDerivativeTable/nonDerivativeTransaction"):
        code = (txn.findtext("transactionCoding/transactionCode") or txn.findtext("transactionCode") or "").strip()
        if code.upper() != "P":
            continue

        shares = to_decimal(txn.findtext("transactionAmounts/transactionShares/value"))
        price = to_decimal(txn.findtext("transactionAmounts/transactionPricePerShare/value"))
        if shares is None or price is None:
            continue
        value = shares * price
        if value < minimum_value:
            continue
        date = (txn.findtext("transactionDate/value") or "").strip()

        purchases.append(
            InsiderPurchase(
                date=date,
                issuer_name=issuer_name or "Unknown Issuer",
                ticker=ticker or "N/A",
                insider=insider or "Unknown",
                shares=shares,
                price=price,
                value=value,
                filing_url="",
            )
        )
    return purchases


def gather_purchases() -> List[InsiderPurchase]:
    feed_xml = fetch_text(SEC_FEED_URL.format(count=FEED_COUNT))
    entries = parse_feed_entries(feed_xml)
    
    # --- THE MEMORY BANK ---
    seen_file = Path(__file__).resolve().parent.parent / "seen_filings.txt"
    seen_links = set()
    if seen_file.exists():
        seen_links = set(seen_file.read_text(encoding="utf-8").splitlines())

    # Filter out filings we have already checked
    new_entries = [e for e in entries if e["link"] not in seen_links]
    
    # STRICT BUDGET: Only process 15 new filings per day to stay under 1,000/month limit
    daily_budget = 15
    new_entries = new_entries[:daily_budget]
    logging.info("Found %d total filings. Processing %d new filings today to save proxy quota.", len(entries), len(new_entries))

    all_purchases: List[InsiderPurchase] = []
    
    for entry in new_entries:
        # 1. Add the link to our memory bank immediately so we never check it again
        seen_links.add(entry["link"])
        
        try:
            index_html = fetch_text(entry["link"])
        except HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else "unknown"
            logging.warning("HTTP error fetching filing index %s (status %s): %s", entry["link"], status, exc)
            continue
        except RequestException as exc:  # noqa: PERF203
            logging.warning("Network error fetching filing index %s: %s", entry["link"], exc)
            continue
        except Exception as exc:  # noqa: BLE001
            logging.warning(
                "Skipping %s due to unexpected index fetch error (%s): %s",
                entry["link"],
                type(exc).__name__,
                exc,
            )
            continue

        xml_href = find_xml_document_href(index_html)
        if not xml_href:
            logging.debug("No XML document found in %s", entry["link"])
            continue

        base_dir = entry["link"].rsplit("/", 1)[0] + "/"
        xml_url = urljoin(base_dir, xml_href)

        try:
            xml_text = fetch_text(xml_url)
            purchases = parse_purchase_transactions(xml_text)
        except HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else "unknown"
            logging.warning("HTTP error fetching ownership XML %s (status %s): %s", xml_url, status, exc)
            continue
        except ET.ParseError as exc:
            logging.warning("XML parse error for %s: %s", xml_url, exc)
            continue
        except Exception as exc:  # noqa: BLE001
            logging.warning("Failed to parse filing %s (%s): %s", xml_url, type(exc).__name__, exc)
            continue

        for purchase in purchases:
            purchase.filing_url = entry["link"]
        all_purchases.extend(purchases)

    # 2. Save the updated memory bank back to the file before finishing
    seen_file.write_text("\n".join(seen_links), encoding="utf-8")

    return sorted(all_purchases, key=lambda p: p.value, reverse=True)


def format_decimal(value: Decimal) -> str:
    quantized = value.quantize(Decimal("0.01"))
    return f"${quantized:,.2f}"


def format_number(value: Decimal) -> str:
    return f"{value:,.0f}"


def write_markdown(purchases: List[InsiderPurchase]) -> None:
    now = datetime.now(timezone.utc)
    summary_text = generate_statistical_summary(purchases) # Generate the summary
    lines = [
        "# Daily Insider Purchases (>$50,000)",
        f"Last updated: {now:%Y-%m-%d %H:%M UTC}",
        f"Data source: SEC Form 4 current filings (latest {FEED_COUNT})",
        "",
        summary_text, # Add it to the markdown
        "",
        "### 🚨 Latest Transactions (>$50k)",
    ]

    if not purchases:
        lines.append("No qualifying insider purchases were found in the latest feed.")
        OUTPUT_FILE.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return

    lines.extend(
        [
            "| Date | Company | Ticker | Insider | Shares | Price | Value | Filing |",
            "| --- | --- | --- | --- | ---: | ---: | ---: | --- |",
        ]
    )

    for purchase in purchases:
        lines.append(
            "| {date} | {company} | {ticker} | {insider} | {shares} | {price} | {value} | [link]({filing}) |".format(
                date=purchase.date or "N/A",
                company=purchase.issuer_name,
                ticker=purchase.ticker,
                insider=purchase.insider,
                shares=format_number(purchase.shares),
                price=format_decimal(purchase.price),
                value=format_decimal(purchase.value),
                filing=purchase.filing_url,
            )
        )

    OUTPUT_FILE.write_text("\n".join(lines) + "\n", encoding="utf-8")

def update_historical_csv(new_purchases: List[InsiderPurchase]) -> None:
    """Appends new purchases to the CSV database without creating duplicates."""
    existing_records = set()
    
    # Load existing signatures to avoid duplicates
    if CSV_FILE.exists() and os.path.getsize(CSV_FILE) > 0:
        with open(CSV_FILE, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader, None) # Skip header
            for row in reader:
                if len(row) >= 7:
                    # Create a unique signature: Date + Ticker + Insider + Value
                    existing_records.add((row[0], row[2], row[3], row[6]))

    # Append new records
    is_new_file = not CSV_FILE.exists() or os.path.getsize(CSV_FILE) == 0
    with open(CSV_FILE, 'a', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        if is_new_file:
            writer.writerow(["Date", "Company", "Ticker", "Insider", "Shares", "Price", "Value", "Filing URL"])

        for p in new_purchases:
            signature = (p.date or "N/A", p.ticker, p.insider, format_decimal(p.value))
            if signature not in existing_records:
                writer.writerow([
                    p.date or "N/A", p.issuer_name, p.ticker,
                    p.insider, str(p.shares), str(p.price),
                    format_decimal(p.value), p.filing_url
                ])

def generate_statistical_summary(purchases: List[InsiderPurchase]) -> str:
    """Generates a quantitative summary of the day's insider activity."""
    if not purchases:
        return "> *No massive insider buys detected in the current feed.*"
    
    total_value = sum(p.value for p in purchases)
    largest_buy = max(purchases, key=lambda p: p.value)
    
    return (
        f"### 📊 Daily Market Pulse\n"
        f"> - **Total Capital Injected:** {format_decimal(total_value)} across {len(purchases)} high-conviction transactions.\n"
        f"> - **Largest Single Move:** {largest_buy.insider} bought **{format_decimal(largest_buy.value)}** of {largest_buy.issuer_name} ({largest_buy.ticker}).\n"
    )

def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    try:
        purchases = gather_purchases()
    except Exception as exc:  # noqa: BLE001
        logging.error(
            "Failed to gather insider purchases (%s). Check network connectivity or SEC feed format changes. Details: %s",
            type(exc).__name__,
            exc,
        )
        raise SystemExit(1)

    update_historical_csv(purchases)
    write_markdown(purchases)
    logging.info("Wrote %s", OUTPUT_FILE)


if __name__ == "__main__":
    main()
