"""Batch ingestion task bodies (source -> Bronze). DAG files only wire these together.

Raw responses are staged on the local `data/raw/` volume between extract and land.
Laptop simplification: in production the staging area would be an object-store prefix.
"""

import json
from datetime import date, timedelta
from pathlib import Path

from lakehouse.config import settings
from lakehouse.ingestion import edgar, yahoo
from lakehouse.ingestion.bronze_writer import overwrite_slice, slice_keys, write_quarantine
from lakehouse.ops import get_watermark, instrumented, record_reconciliation, set_watermark

MAX_QUARANTINE_RATE = 0.05
PRICES_TABLE = "bronze.prices_eod_raw"
FACTS_TABLE = "bronze.sec_companyfacts_raw"
DAILY_LOOKBACK_DAYS = 7  # refetch the last week each day to pick up late corrections from the vendor


class QuarantineRateExceeded(Exception):
    def __init__(self, message: str, stats: dict):
        super().__init__(message)
        self.stats = stats  # @instrumented records these on the failed ops.pipeline_run row


def _raw_dir(source: str, logical_date: date) -> Path:
    d = Path(settings().data_dir) / "raw" / source / logical_date.isoformat()
    d.mkdir(parents=True, exist_ok=True)
    return d


def prices_window(logical_date: date) -> tuple[date, date]:
    """Deterministic fetch window derived from the logical date (not from a moving watermark),
    so a rerun of the same date fetches the same range."""
    return logical_date - timedelta(days=DAILY_LOOKBACK_DAYS), logical_date


# ---- Yahoo Finance daily prices -------------------------------------------------------------------

@instrumented(source="yfinance", entity="prices_eod")
def extract_prices(*, ticker: str, start: date, end: date, logical_date: date, source: str) -> str:
    records = yahoo.fetch_daily(ticker, start, end)
    path = _raw_dir(source, logical_date) / f"{ticker}.json"
    path.write_text(json.dumps(records))
    return str(path)


def _parse_price_files(paths: list[str]) -> tuple[list[dict], list[dict]]:
    valid, bad = [], []
    for p in paths:
        v, b = yahoo.parse_daily(Path(p).stem, json.loads(Path(p).read_text()))
        valid += v
        bad += b
    return valid, bad


def _check_quarantine_rate(n_valid: int, n_bad: int) -> None:
    total = n_valid + n_bad
    if total and n_bad / total > MAX_QUARANTINE_RATE:
        raise QuarantineRateExceeded(f"quarantine rate {n_bad}/{total} exceeds {MAX_QUARANTINE_RATE:.0%}",
                                     {"rows_read": total, "rows_written": 0, "rows_quarantined": n_bad})


@instrumented(source="yfinance", entity="prices_eod")
def land_prices_bronze(*, paths: list[str], logical_date: date, run_id: str, source: str) -> dict:
    valid, bad = _parse_price_files(paths)
    write_quarantine(bad, source=source, entity=None, logical_date=logical_date, run_id=run_id)
    # Check *before* touching Bronze: a 100 %-quarantined run (schema drift) must fail without replacing the
    # slice with nothing. The rejected rows are already in quarantine, so they stay inspectable.
    _check_quarantine_rate(len(valid), len(bad))
    written = overwrite_slice(PRICES_TABLE, valid, source=source, logical_date=logical_date,
                              run_id=run_id, payload_col="_raw_line")
    if valid:
        set_watermark(source, "prices_eod", max(r["trade_date"] for r in valid).isoformat())
    return {"rows_read": len(valid) + len(bad), "rows_written": written, "rows_quarantined": len(bad)}


@instrumented(source="yfinance", entity="prices_eod")
def reconcile_prices_source_bronze(*, paths: list[str], logical_date: date, run_id: str, source: str) -> dict:
    valid, _ = _parse_price_files(paths)
    expected = {(r["ticker"], r["trade_date"]) for r in valid}
    keys = slice_keys(PRICES_TABLE, ["ticker", "trade_date"], source=source, logical_date=logical_date)
    status = record_reconciliation(
        run_id=run_id, source=source, entity="prices_eod", logical_date=logical_date,
        layer_from="source", layer_to="bronze", count_from=len(valid), count_to=len(keys),
        missing=len(expected - set(keys)), duplicates=len(keys) - len(set(keys)),
    )
    return {"rows_read": len(valid), "rows_written": len(keys), "status": status}


# ---- SEC EDGAR companyfacts -----------------------------------------------------------------------

@instrumented(source="sec_edgar", entity="companyfacts")
def extract_edgar(*, cik: str, ticker: str, logical_date: date, source: str) -> dict | None:
    """Fetch companyfacts only if `submissions` shows a 10-K/10-Q newer than this CIK's watermark."""
    latest = edgar.latest_periodic_filing_date(edgar.fetch_submissions(cik))
    seen = get_watermark(source, f"cik:{cik}")
    if latest is None or (seen is not None and latest <= seen):
        return None
    path = _raw_dir(source, logical_date) / f"{cik}.json"
    path.write_text(edgar.fetch_companyfacts(cik))
    return {"cik": cik, "ticker": ticker, "path": str(path), "latest_filing": latest}


@instrumented(source="sec_edgar", entity="companyfacts")
def land_edgar_bronze(*, fetched: list[dict], logical_date: date, run_id: str, source: str) -> dict:
    """Overwrite only the CIKs fetched in this run: a rerun that skips every CIK is then a no-op."""
    fetched = [f for f in fetched if f]
    if not fetched:
        return {"rows_read": 0, "rows_written": 0, "rows_quarantined": 0}
    valid, bad = [], []
    for f in fetched:
        payload = Path(f["path"]).read_text()
        reason = edgar.validate_companyfacts(payload)
        if reason:
            bad.append({"entity": f["cik"], "reason": reason, "_payload": payload[:10_000]})
        else:
            valid.append({"cik": f["cik"], "ticker": f["ticker"], "_payload": payload,
                          "payload_bytes": len(payload.encode())})
    ciks = [f["cik"] for f in fetched]
    for b in bad:
        write_quarantine([b], source=source, entity=b["entity"], logical_date=logical_date, run_id=run_id)
    _check_quarantine_rate(len(valid), len(bad))  # before overwriting Bronze (see land_prices_bronze)
    written = overwrite_slice(FACTS_TABLE, valid, source=source, logical_date=logical_date, run_id=run_id,
                              payload_col="_payload", cik=ciks)
    for f in fetched:
        if f["cik"] in {v["cik"] for v in valid}:
            set_watermark(source, f"cik:{f['cik']}", f["latest_filing"])
    return {"rows_read": len(fetched), "rows_written": written, "rows_quarantined": len(bad)}


@instrumented(source="sec_edgar", entity="companyfacts")
def reconcile_edgar_source_bronze(*, fetched: list[dict], logical_date: date, run_id: str, source: str) -> dict:
    ciks = [f["cik"] for f in fetched if f]
    keys = slice_keys(FACTS_TABLE, ["cik"], source=source, logical_date=logical_date, cik=ciks) if ciks else []
    quarantined = len([c for c in ciks if c not in set(k[0] for k in keys)])
    status = record_reconciliation(
        run_id=run_id, source=source, entity="companyfacts", logical_date=logical_date,
        layer_from="source", layer_to="bronze", count_from=len(ciks) - quarantined, count_to=len(keys),
        duplicates=len(keys) - len(set(keys)),
    )
    return {"rows_read": len(ciks), "rows_written": len(keys), "status": status}

