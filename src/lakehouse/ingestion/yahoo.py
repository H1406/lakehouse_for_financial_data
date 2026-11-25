"""Yahoo Finance (via yfinance): daily history for the batch path.

yfinance scrapes Yahoo's unofficial API: no key, no documented limit, but it throttles (YFRateLimitError).
We send one request per ticker per run and retry throttling with exponential backoff.
Note: with auto_adjust=False, Open/High/Low/Close are already split-adjusted (not the printed price);
`Adj Close` also includes dividends. `Stock Splits` is 0.0 on days without a split.
"""

import json
import math
from datetime import date, timedelta

import pandas as pd
import yfinance as yf
from pydantic import ValidationError
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential
from yfinance.exceptions import YFRateLimitError

from lakehouse.ingestion import http
from lakehouse.ingestion.contracts import YAHOO_DAILY_COLUMNS, YahooDailyRow

yf.config.debug.hide_exceptions = False  # raise instead of printing and returning an empty frame
_limiter = http.RateLimiter(per_second=1)


def to_records(df: pd.DataFrame, time_col: str, time_fmt: str) -> list[dict]:
    """DataFrame -> JSON-safe dicts with yfinance's own column names (NaN -> None, numpy -> python)."""
    out = []
    for ts, row in df.iterrows():
        rec = {time_col: ts.strftime(time_fmt)}
        for k, v in row.items():
            v = v.item() if hasattr(v, "item") else v
            rec[k] = None if isinstance(v, float) and math.isnan(v) else v
        out.append(rec)
    return out


@retry(retry=retry_if_exception_type((YFRateLimitError, ConnectionError)), reraise=True,
       wait=wait_exponential(multiplier=2, max=60), stop=stop_after_attempt(5))
def fetch_daily(ticker: str, start: date, end: date) -> list[dict]:
    """Daily bars for [start, end] inclusive (yfinance's `end` is exclusive)."""
    _limiter.wait()
    df = yf.Ticker(ticker).history(start=start.isoformat(), end=(end + timedelta(days=1)).isoformat(),
                                   interval="1d", auto_adjust=False, actions=True)
    # Date index is midnight America/New_York; format it as the local trading date, never via UTC.
    return to_records(df, "Date", "%Y-%m-%d")


def parse_daily(ticker: str, records: list[dict]) -> tuple[list[dict], list[dict]]:
    """Return (valid rows, quarantined rows). Columns that differ from the contract quarantine every row."""
    if not records:
        return [], []
    if set(records[0]) != set(YAHOO_DAILY_COLUMNS):
        reason = f"column mismatch: expected {sorted(YAHOO_DAILY_COLUMNS)}, got {sorted(records[0])}"
        return [], [{"entity": ticker, "reason": reason, "_payload": json.dumps(r, sort_keys=True)} for r in records]

    valid, bad = [], []
    for rec in records:
        raw = json.dumps(rec, sort_keys=True)
        try:
            row = YahooDailyRow.model_validate(rec)
        except ValidationError as exc:
            reason = "; ".join(f"{'.'.join(map(str, e['loc']))}: {e['msg']}" for e in exc.errors())[:500]
            bad.append({"entity": ticker, "reason": reason, "_payload": raw})
            continue
        valid.append({"ticker": ticker, **row.model_dump(), "_raw_line": raw})
    return valid, bad
