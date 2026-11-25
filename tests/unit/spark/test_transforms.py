from datetime import date, datetime
from pathlib import Path

import pytest
from pyspark.sql import Row

from spark_jobs.bronze_to_silver_facts import KEY, explode_facts
from spark_jobs.common import latest_per_key

FIXTURE = Path(__file__).parents[2] / "fixtures" / "edgar_companyfacts_aapl.json"


def test_latest_per_key_keeps_newest_ingest(spark):
    rows = [Row(ticker="AAPL", trade_date=date(2026, 10, 1), close=1.0, _ingested_at=datetime(2026, 10, 1),
                _payload_hash="a"),
            Row(ticker="AAPL", trade_date=date(2026, 10, 1), close=2.0, _ingested_at=datetime(2026, 10, 2),
                _payload_hash="b")]
    out = latest_per_key(spark.createDataFrame(rows), ["ticker", "trade_date"]).collect()
    assert [r["close"] for r in out] == [2.0]


def test_explode_recorded_companyfacts(spark):
    if not FIXTURE.exists():
        pytest.skip("edgar_companyfacts_aapl.json not recorded yet")
    bronze = spark.createDataFrame([Row(cik="0000320193", ticker="AAPL", _payload=FIXTURE.read_text(),
                                        _run_id="r", _source="sec_edgar", _ingested_at=datetime(2026, 11, 25))])
    facts = explode_facts(bronze)
    assert facts.count() > 0
    assert facts.where("period_end < '2019-01-01'").count() == 0
    assert facts.where("concept is null or unit is null or accession_no is null or filed_date is null").count() == 0
    assert facts.select(*KEY).distinct().count() <= facts.count()
