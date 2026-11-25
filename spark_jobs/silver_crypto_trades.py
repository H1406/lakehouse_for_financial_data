"""bronze.crypto_trades -> silver.crypto_trades, deduped on (symbol, trade_id). Runs every 15 minutes.

Reprocesses a trailing window (default 6 h) so trades that landed late after a stream outage are still
picked up; MERGE makes the overlap free of duplicates.
"""

import argparse
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from lakehouse.ops import instrumented, record_reconciliation
from spark_jobs.common import latest_per_key, spark_session

KEY = ["symbol", "trade_id"]


@instrumented(source="binance", entity="crypto_trades")
def run(spark: SparkSession, *, until: datetime, lookback_hours: int, run_id: str) -> dict:
    since = until - timedelta(hours=lookback_hours)
    in_window = (F.col("trade_time") >= F.lit(since)) & (F.col("trade_time") < F.lit(until))
    bronze = spark.table("lake.bronze.crypto_trades").where(in_window)
    src = (latest_per_key(bronze, KEY)
           .drop("kafka_partition", "kafka_offset")
           .withColumn("_updated_at", F.current_timestamp())
           .cache())
    src.createOrReplaceTempView("src")
    spark.sql(f"""
        merge into lake.silver.crypto_trades t
        using src s
        on t.symbol = s.symbol and t.trade_id = s.trade_id
           and t.trade_time >= timestamp '{since:%Y-%m-%d %H:%M:%S}'
        when not matched then insert *
    """)
    bronze_rows, distinct_keys = bronze.count(), src.count()
    silver_rows = spark.table("lake.silver.crypto_trades").where(in_window).count()
    # Bronze is at-least-once, so replayed rows after a restart are expected; they are returned as
    # bronze_replayed_rows. Reconciliation compares distinct Bronze keys with Silver rows.
    status = record_reconciliation(
        run_id=run_id, source="binance", entity="crypto_trades", logical_date=until.date(),
        layer_from="bronze", layer_to="silver", count_from=distinct_keys, count_to=silver_rows,
        missing=max(distinct_keys - silver_rows, 0), duplicates=max(silver_rows - distinct_keys, 0),
    )
    return {"rows_read": bronze_rows, "rows_written": distinct_keys, "status": status,
            "bronze_replayed_rows": bronze_rows - distinct_keys}


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--until", required=True, help="ISO timestamp, exclusive upper bound (UTC)")
    p.add_argument("--lookback-hours", type=int, default=6)
    p.add_argument("--run-id", required=True)
    a = p.parse_args()
    until = datetime.fromisoformat(a.until).replace(tzinfo=None)  # session time zone is UTC
    run(spark_session("silver_crypto_trades"), until=until, lookback_hours=a.lookback_hours, run_id=a.run_id)
