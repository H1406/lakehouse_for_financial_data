import json

from lakehouse.ingestion.contracts import YAHOO_DAILY_COLUMNS
from lakehouse.ingestion.yahoo import parse_daily

from .conftest import fixture_or_skip


def daily_fixture() -> list[dict]:
    return json.loads(fixture_or_skip("yahoo_aapl_daily.json").read_text())


def test_recorded_daily_is_all_valid_and_maps_splits():
    valid, bad = parse_daily("AAPL", daily_fixture())
    assert valid and not bad
    by_date = {r["trade_date"].isoformat(): r for r in valid}
    assert by_date["2020-08-31"]["split_factor"] == 4.0  # AAPL 4:1 split
    assert by_date["2026-10-01"]["split_factor"] == 1.0  # yfinance 0.0 -> 1.0 (no split)
    assert all(r["adj_close"] > 0 and isinstance(r["volume"], int) for r in valid)
    assert json.loads(valid[0]["_raw_line"])["Date"] == valid[0]["trade_date"].isoformat()


def test_bad_rows_are_quarantined():
    rows = daily_fixture()[:3]
    rows[1] = {**rows[1], "Close": None}
    rows[2] = {**rows[2], "High": 1.0, "Low": 2.0}
    valid, bad = parse_daily("AAPL", rows)
    assert len(valid) == 1 and len(bad) == 2
    assert "high < low" in bad[1]["reason"]


def test_column_drift_quarantines_everything():
    rows = [{("AdjClose" if k == "Adj Close" else k): v for k, v in r.items()} for r in daily_fixture()[:2]]
    valid, bad = parse_daily("AAPL", rows)
    assert not valid and len(bad) == 2 and "column mismatch" in bad[0]["reason"]


def test_empty():
    assert parse_daily("AAPL", []) == ([], [])
    assert len(YAHOO_DAILY_COLUMNS) == 9
