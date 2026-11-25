import json
from datetime import datetime, timedelta

from lakehouse.ingestion.contracts import YahooMinuteBar
from streaming import equity_producer as ep

from .conftest import fixture_or_skip


def minute_fixture() -> list[dict]:
    return json.loads(fixture_or_skip("yahoo_aapl_1m.json").read_text())


def bar_time(rec: dict) -> datetime:
    return datetime.strptime(rec["Datetime"], ep.TIME_FMT)


def test_only_complete_bars_older_than_delay_are_released():
    recs = minute_fixture()
    first, last = bar_time(recs[0]), bar_time(recs[-1])
    # "now" = 5 minutes after the last bar ends: with a 10-minute delay the last bars are still embargoed.
    now = last + timedelta(minutes=6)
    msgs = ep.releasable("AAPL", recs, None, now)
    released = [datetime.fromisoformat(m["minute"]) for m in msgs]
    assert released and released[0] == first
    assert all(t + timedelta(minutes=1) + ep.DELAY <= now for t in released)
    assert len(msgs) == len(recs) - 5
    assert all(m["delay_minutes"] == 10 for m in msgs)
    for m in msgs:
        YahooMinuteBar.model_validate(m)  # producer output honours the topic contract


def test_already_sent_bars_are_not_republished():
    recs = minute_fixture()
    now = bar_time(recs[-1]) + timedelta(hours=1)
    sent = bar_time(recs[9])
    msgs = ep.releasable("AAPL", recs, sent, now)
    assert len(msgs) == len(recs) - 10
    assert all(datetime.fromisoformat(m["minute"]) > sent for m in msgs)
