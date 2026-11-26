"""Iceberg table maintenance (only ever run from the iceberg_maintenance DAG).

Compacts the small files the stream produces, expires snapshots older than 7 days and removes orphans.
The stream keeps committing meanwhile, so a maintenance commit can lose the optimistic-concurrency race
("CommitFailedException: branch main has changed"). Mitigations: compaction commits file groups separately
(partial-progress), each table/procedure is isolated, and the run fails once at the end with a precise list,
instead of aborting on the first conflict with an unrelated error.
"""

from datetime import UTC, datetime, timedelta

from lakehouse.ops import instrumented
from spark_jobs.common import spark_session

COMPACT = ["bronze.crypto_trades", "bronze.equity_bars_1m", "silver.crypto_trades", "silver.crypto_bars_1m",
           "silver.equity_bars_1m"]
ALL = [*COMPACT, "bronze.prices_eod_raw", "bronze.sec_companyfacts_raw", "bronze.quarantine",
       "silver.prices_eod", "silver.financial_facts"]


class MaintenanceFailed(Exception):
    pass


@instrumented(source="lake", entity="maintenance")
def run() -> dict:
    spark = spark_session("iceberg_maintenance")
    expire_before = (datetime.now(UTC) - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
    orphan_before = (datetime.now(UTC) - timedelta(days=3)).strftime("%Y-%m-%d %H:%M:%S")
    calls = [(t, "rewrite_data_files", f"table => 'lake.{t}', options => map('target-file-size-bytes', "
                                       f"'134217728', 'partial-progress.enabled', 'true')") for t in COMPACT]
    calls += [(t, "expire_snapshots", f"table => 'lake.{t}', older_than => timestamp '{expire_before}', "
                                      f"retain_last => 5") for t in ALL]
    # prefix_listing: list files through S3FileIO, so no Hadoop S3 filesystem is needed.
    calls += [(t, "remove_orphan_files", f"table => 'lake.{t}', older_than => timestamp '{orphan_before}', "
                                         f"prefix_listing => true") for t in ALL]
    rewritten, failures = 0, []
    for table, proc, args in calls:
        try:
            r = spark.sql(f"call lake.system.{proc}({args})").first()
            if proc == "rewrite_data_files":
                rewritten += r["rewritten_data_files_count"]
                print(table, r.asDict())
        except Exception as exc:  # noqa: BLE001 - keep maintaining the other tables
            msg = str(exc).splitlines()[0][:300]
            failures.append(f"{proc}({table}): {msg}")
            print("FAILED", failures[-1])
    if failures:
        raise MaintenanceFailed(f"{len(failures)} of {len(calls)} maintenance calls failed: " + " | ".join(failures))
    return {"rows_read": rewritten, "rows_written": rewritten}


if __name__ == "__main__":
    run()
