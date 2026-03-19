#!/usr/bin/env python3
from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Iterable, List, Optional
from urllib.parse import urljoin
from xml.etree import ElementTree as ET

import requests

SEC_FEED_URL = (
    "https://www.sec.gov/cgi-bin/browse-edgar?"
    "action=getcurrent&CIK=&type=4&company=&dateb=&owner=only&start=0&count={count}&output=atom"
)
USER_AGENT = os.environ.get(
    "SEC_USER_AGENT",
    "FollowTheMoneyBot/1.0 (https://github.com/gautam00010/Follow-The-Money)",
)
OUTPUT_FILE = Path(__file__).resolve().parent.parent / "daily_report.md"
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
    headers = {"User-Agent": USER_AGENT}
    logging.debug("Fetching %s", url)
    response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
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
    logging.info("Found %d recent Form 4 filings in feed", len(entries))

    all_purchases: List[InsiderPurchase] = []
    for entry in entries:
        try:
            index_html = fetch_text(entry["link"])
        except Exception as exc:  # noqa: BLE001
            logging.warning("Skipping %s due to index fetch error: %s", entry["link"], exc)
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
        except Exception as exc:  # noqa: BLE001
            logging.warning("Failed to parse filing %s: %s", xml_url, exc)
            continue

        for purchase in purchases:
            purchase.filing_url = entry["link"]
        all_purchases.extend(purchases)

    return sorted(all_purchases, key=lambda p: p.value, reverse=True)


def format_decimal(value: Decimal) -> str:
    quantized = value.quantize(Decimal("0.01"))
    return f"${quantized:,.2f}"


def format_number(value: Decimal) -> str:
    return f"{value:,.0f}"


def write_markdown(purchases: List[InsiderPurchase]) -> None:
    now = datetime.now(timezone.utc)
    lines = [
        "# Daily Insider Purchases (>$50,000)",
        f"Last updated: {now:%Y-%m-%d %H:%M UTC}",
        f"Data source: SEC Form 4 current filings (latest {FEED_COUNT})",
        "",
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


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    try:
        purchases = gather_purchases()
    except Exception as exc:  # noqa: BLE001
        logging.error("Failed to gather insider purchases: %s", exc)
        raise SystemExit(1)

    write_markdown(purchases)
    logging.info("Wrote %s", OUTPUT_FILE)


if __name__ == "__main__":
    main()
