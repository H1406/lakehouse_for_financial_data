"""Record real API responses into tests/fixtures/ so parsers are built against reality, not guesses.

Usage: python scripts/record_fixtures.py [yahoo] [edgar] [binance]   (default: all)
Reads SEC_USER_AGENT from env / .env (Yahoo and Binance need no key). Large EDGAR payloads are trimmed to a
few concepts and recent filings; the JSON structure is kept unchanged.
"""

import asyncio
import json
import os
import sys
from pathlib import Path

import requests

OUT = Path(__file__).resolve().parents[1] / "tests" / "fixtures"
KEEP_CONCEPTS = {
    "Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax", "NetIncomeLoss", "OperatingIncomeLoss",
    "Assets", "Liabilities", "StockholdersEquity", "EarningsPerShareDiluted",
}


def load_dotenv() -> None:
    env = Path(__file__).resolve().parents[1] / ".env"
    if env.exists():
        for line in env.read_text().splitlines():
            if "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())


def yahoo() -> None:
    """Stored exactly as the pipeline stages it (lakehouse.ingestion.yahoo.to_records)."""
    from datetime import date

    import yfinance as yf

    from lakehouse.ingestion.yahoo import fetch_daily, to_records

    daily = fetch_daily("AAPL", date(2026, 10, 1), date(2026, 10, 16))
    daily += fetch_daily("AAPL", date(2020, 8, 27), date(2020, 9, 1))  # includes the 4:1 split on 2020-08-31
    (OUT / "yahoo_aapl_daily.json").write_text(json.dumps(daily, indent=1))
    bars = yf.Ticker("AAPL").history(period="1d", interval="1m", auto_adjust=False, actions=False, prepost=False)
    minute = to_records(bars.tail(30), "Datetime", "%Y-%m-%dT%H:%M:%S%z")
    (OUT / "yahoo_aapl_1m.json").write_text(json.dumps(minute, indent=1))
    print("yahoo:", daily[0], minute[0])


def edgar() -> None:
    headers = {"User-Agent": os.environ["SEC_USER_AGENT"]}
    tickers = requests.get("https://www.sec.gov/files/company_tickers.json", headers=headers, timeout=30).json()
    first = dict(list(tickers.items())[:20])
    (OUT / "edgar_company_tickers.json").write_text(json.dumps(first, indent=1))

    sub = requests.get("https://data.sec.gov/submissions/CIK0000320193.json", headers=headers, timeout=30).json()
    sub["filings"]["recent"] = {k: v[:25] for k, v in sub["filings"]["recent"].items()}
    (OUT / "edgar_submissions_aapl.json").write_text(json.dumps(sub, indent=1))

    facts = requests.get(
        "https://data.sec.gov/api/xbrl/companyfacts/CIK0000320193.json", headers=headers, timeout=60
    ).json()
    facts["facts"]["us-gaap"] = {k: v for k, v in facts["facts"]["us-gaap"].items() if k in KEEP_CONCEPTS}
    (OUT / "edgar_companyfacts_aapl.json").write_text(json.dumps(facts))
    print("edgar: taxonomies", list(facts["facts"]), "concepts", list(facts["facts"]["us-gaap"]))


async def binance(n: int = 30) -> None:
    import ssl

    import certifi
    import websockets

    url = "wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade"
    async with websockets.connect(url, ssl=ssl.create_default_context(cafile=certifi.where())) as ws:
        msgs = [json.loads(await ws.recv()) for _ in range(n)]
    (OUT / "binance_trades.jsonl").write_text("\n".join(json.dumps(m) for m in msgs) + "\n")
    print("binance:", msgs[0])


if __name__ == "__main__":
    load_dotenv()
    OUT.mkdir(parents=True, exist_ok=True)
    which = sys.argv[1:] or ["yahoo", "edgar", "binance"]
    if "yahoo" in which:
        yahoo()
    if "edgar" in which:
        edgar()
    if "binance" in which:
        asyncio.run(binance())
