"""Running the same logical date twice must leave Bronze unchanged and reconcile ok (live stack)."""

import json
import shutil
from datetime import date
from pathlib import Path

from lakehouse.ingestion import tasks
from lakehouse.ingestion.bronze_writer import overwrite_slice, slice_keys
from lakehouse.ops import connect

SOURCE, DAY = "it_test", date(1999, 1, 4)  # a slice no real pipeline writes
FIXTURE = Path(__file__).parents[1] / "fixtures" / "yahoo_aapl_daily.json"


def test_land_twice_is_a_noop_and_reconciles(tmp_path):
    staged = tmp_path / "AAPL.json"
    shutil.copy(FIXTURE, staged)
    expected = len(json.loads(FIXTURE.read_text()))
    try:
        for run in ("it_run_1", "it_run_2"):
            tasks.land_prices_bronze(paths=[str(staged)], logical_date=DAY, run_id=run, source=SOURCE)
            keys = slice_keys(tasks.PRICES_TABLE, ["ticker", "trade_date"], source=SOURCE, logical_date=DAY)
            assert len(keys) == expected and len(set(keys)) == expected  # replaced, never appended
            tasks.reconcile_prices_source_bronze(paths=[str(staged)], logical_date=DAY, run_id=run, source=SOURCE)
        with connect() as c:
            statuses = c.execute("select status from ops.reconciliation where source = %s and run_id like 'it_run_%%' "
                                 "order by checked_at desc limit 2", (SOURCE,)).fetchall()
        assert [s[0] for s in statuses] == ["ok", "ok"]
    finally:
        overwrite_slice(tasks.PRICES_TABLE, [], source=SOURCE, logical_date=DAY, run_id="it_cleanup",
                        payload_col="_raw_line")  # remove the test slice
        with connect() as c:
            c.execute("delete from ops.reconciliation where source = %s", (SOURCE,))
