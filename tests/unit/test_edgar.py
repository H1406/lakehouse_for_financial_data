import json

from lakehouse.ingestion.edgar import cik10, latest_periodic_filing_date, validate_companyfacts

from .conftest import fixture_or_skip


def test_cik10():
    assert cik10("320193") == "0000320193" and cik10(320193) == "0000320193"


def test_latest_filing_from_recorded_submissions():
    sub = json.loads(fixture_or_skip("edgar_submissions_aapl.json").read_text())
    latest = latest_periodic_filing_date(sub)
    recent = sub["filings"]["recent"]
    periodic = [d for f, d in zip(recent["form"], recent["filingDate"], strict=True) if f in {"10-K", "10-Q"}]
    assert latest == max(periodic)


def test_recorded_companyfacts_passes_contract():
    assert validate_companyfacts(fixture_or_skip("edgar_companyfacts_aapl.json").read_text()) is None


def test_bad_payload_reason():
    assert validate_companyfacts('{"cik": 1, "entityName": "X", "facts": {"dei": {}}}')
    assert validate_companyfacts("{not json")
