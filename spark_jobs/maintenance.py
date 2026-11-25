"""Iceberg table maintenance (only ever run from the iceberg_maintenance DAG).

Compacts the small files the stream produces, expires snapshots older than 7 days and removes orphans.
"""

from datetime import UTC, datetime, timedelta

from lakehouse.ops import instrumented
from spark_jobs.common import spark_session

COMPACT = ["bronze.crypto_trades", "silver.crypto_trades", "silver.crypto_bars_1m"]
ALL = [*COMPACT, "bronze.prices_eod_raw", "bronze.sec_companyfacts_raw", "bronze.quarantine",
       "silver.prices_eod", "silver.financial_facts"]


@instrumented(source="lake", entity="maintenance")
def run() -> dict:
    spark = spark_session("iceberg_maintenance")
    expire_before = (datetime.now(UTC) - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
    orphan_before = (datetime.now(UTC) - timedelta(days=3)).strftime("%Y-%m-%d %H:%M:%S")
    rewritten = 0
    for t in COMPACT:
        r = spark.sql(f"call lake.system.rewrite_data_files(table => 'lake.{t}', "
                      f"options => map('target-file-size-bytes', '134217728'))").first()
        rewritten += r["rewritten_data_files_count"]
        print(t, r.asDict())
    for t in ALL:
        spark.sql(f"call lake.system.expire_snapshots(table => 'lake.{t}', older_than => timestamp '{expire_before}', "
                  "retain_last => 5)")
        # prefix_listing: list files through S3FileIO, so no Hadoop S3 filesystem is needed.
        spark.sql(f"call lake.system.remove_orphan_files(table => 'lake.{t}', "
                  f"older_than => timestamp '{orphan_before}', prefix_listing => true)")
    return {"rows_read": rewritten, "rows_written": rewritten}


if __name__ == "__main__":
    run()
