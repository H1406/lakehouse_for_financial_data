"""Bronze prices slice -> silver.prices_eod via MERGE INTO on (ticker, trade_date), then reconcile."""

from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lakehouse.ops import instrumented, record_reconciliation
from spark_jobs.common import batch_args, latest_per_key, spark_session

KEY = ["ticker", "trade_date"]


def prepare(bronze_slice: DataFrame) -> DataFrame:
    return latest_per_key(bronze_slice, KEY).drop("_raw_line", "_logical_date").withColumn(
        "_updated_at", F.current_timestamp()
    )


@instrumented(source="yfinance", entity="prices_eod")
def run(spark: SparkSession, *, logical_date: date, run_id: str, source: str) -> dict:
    bronze = spark.table("lake.bronze.prices_eod_raw").where(
        (F.col("_source") == source) & (F.col("_logical_date") == F.lit(logical_date))
    )
    src = prepare(bronze).cache()
    src.createOrReplaceTempView("src")
    spark.sql("""
        merge into lake.silver.prices_eod t
        using src s
        on t.ticker = s.ticker and t.trade_date = s.trade_date
        when matched and s._ingested_at >= t._ingested_at and s._payload_hash <> t._payload_hash
          then update set *
        when not matched then insert *
    """)

    bronze_rows = bronze.count()
    distinct_keys = src.count()
    silver_rows = spark.table("lake.silver.prices_eod").join(src.select(*KEY), KEY, "left_semi").count()
    status = record_reconciliation(
        run_id=run_id, source=source, entity="prices_eod", logical_date=logical_date,
        layer_from="bronze", layer_to="silver", count_from=distinct_keys, count_to=silver_rows,
        missing=distinct_keys - silver_rows, duplicates=bronze_rows - distinct_keys,
    )
    return {"rows_read": bronze_rows, "rows_written": distinct_keys, "status": status}


if __name__ == "__main__":
    args = batch_args()
    run(spark_session("bronze_to_silver_prices"), logical_date=date.fromisoformat(args.logical_date),
        run_id=args.run_id, source=args.source)
