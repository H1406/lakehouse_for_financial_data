"""Long-running Spark Structured Streaming service: Redpanda -> Iceberg, three queries in one app.

Query A (bronze_trades): parse with an explicit schema; good rows -> bronze.crypto_trades, bad rows ->
  bronze.quarantine, per-batch metrics -> ops.stream_metrics. foreachBatch is at-least-once on replay,
  so Silver dedups by (symbol, trade_id).
Query B (bars_1m): 2-minute event-time watermark + 1-minute tumbling window -> silver.crypto_bars_1m
  (append mode: a window is emitted once, after the watermark passes its end).
Query C (equity_bars): delayed (>= 10 min) yfinance 1m bars from topic `equity_bars_1m`; good rows ->
  bronze.equity_bars_1m (raw, at-least-once), then MERGE into silver.equity_bars_1m on (ticker, minute), so
  bars re-published after a producer restart are absorbed. Metrics -> ops.stream_metrics.
Checkpoints live in s3a://lake/checkpoints/<query>. Changing a query's logic may need a new checkpoint path.
"""

import os

# This file runs on the Spark image (Python 3.10): use timezone.utc, not datetime.UTC.
from datetime import datetime, timedelta, timezone

import psycopg
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from lakehouse.config import settings
from spark_jobs.common import latest_per_key, spark_session

TOPIC = os.getenv("KAFKA_TOPIC", "crypto_trades")
EQUITY_TOPIC = os.getenv("EQUITY_TOPIC", "equity_bars_1m")
# v2: new path after moving the catalog from SQLite to Postgres (old offsets pointed at tables that no longer exist).
CHECKPOINTS = os.getenv("CHECKPOINT_ROOT", "s3a://lake/checkpoints/v2")
WATERMARK = "2 minutes"
TRIGGER = "30 seconds"

# Matches tests/fixtures/binance_trades.jsonl (`data` object of the combined stream).
TRADE_SCHEMA = T.StructType([
    T.StructField("e", T.StringType()), T.StructField("E", T.LongType()), T.StructField("s", T.StringType()),
    T.StructField("t", T.LongType()), T.StructField("p", T.StringType()), T.StructField("q", T.StringType()),
    T.StructField("T", T.LongType()), T.StructField("m", T.BooleanType()),
])

# Matches the message built by streaming/equity_producer.py (contract: YahooMinuteBar).
EQUITY_SCHEMA = T.StructType([
    T.StructField("ticker", T.StringType()), T.StructField("minute", T.StringType()),
    T.StructField("open", T.DoubleType()), T.StructField("high", T.DoubleType()),
    T.StructField("low", T.DoubleType()), T.StructField("close", T.DoubleType()),
    T.StructField("volume", T.LongType()), T.StructField("published_at", T.LongType()),
    T.StructField("delay_minutes", T.IntegerType()),
])
EQUITY_COLS = ["ticker", "minute", "open", "high", "low", "close", "volume", "published_at"]


def parse(kafka_df: DataFrame) -> DataFrame:
    """Kafka rows -> typed trades plus `_valid` flag and the raw JSON for quarantine."""
    j = F.from_json(F.col("json"), TRADE_SCHEMA, {"mode": "PERMISSIVE"})
    df = kafka_df.select(F.col("value").cast("string").alias("json"), F.col("partition"), F.col("offset"))
    df = df.withColumn("d", j)
    df = df.select(
        "json",
        F.col("d.s").alias("symbol"),
        F.col("d.t").alias("trade_id"),
        F.col("d.p").cast("double").alias("price"),
        F.col("d.q").cast("double").alias("qty"),
        F.timestamp_millis(F.col("d.T")).alias("trade_time"),
        F.timestamp_millis(F.col("d.E")).alias("event_time"),
        F.col("d.m").alias("is_buyer_maker"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
    )
    valid = (
        F.col("symbol").isNotNull() & F.col("trade_id").isNotNull() & F.col("trade_time").isNotNull()
        & (F.col("price") > 0) & (F.col("qty") > 0)
    )
    return df.withColumn("_valid", F.coalesce(valid, F.lit(False)))


def parse_equity(kafka_df: DataFrame) -> DataFrame:
    """Kafka rows -> typed delayed equity bars plus `_valid` flag and the raw JSON for quarantine."""
    df = kafka_df.select(F.col("value").cast("string").alias("json"), F.col("partition"), F.col("offset"))
    df = df.withColumn("d", F.from_json(F.col("json"), EQUITY_SCHEMA, {"mode": "PERMISSIVE"}))
    df = df.select(
        "json",
        F.col("d.ticker").alias("ticker"),
        F.to_timestamp("d.minute").alias("minute"),
        *[F.col(f"d.{c}").alias(c) for c in ("open", "high", "low", "close", "volume")],
        F.timestamp_millis(F.col("d.published_at")).alias("published_at"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
    )
    valid = (
        F.col("ticker").isNotNull() & F.col("minute").isNotNull() & F.col("published_at").isNotNull()
        & (F.col("open") > 0) & (F.col("close") > 0) & (F.col("low") > 0) & (F.col("high") >= F.col("low"))
        & (F.col("volume") >= 0)
    )
    return df.withColumn("_valid", F.coalesce(valid, F.lit(False)))


def to_bars(trades: DataFrame) -> DataFrame:
    return (
        trades.withWatermark("trade_time", WATERMARK)
        .groupBy(F.window("trade_time", "1 minute"), "symbol")
        .agg(
            F.min_by("price", "trade_time").alias("open"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.max_by("price", "trade_time").alias("close"),
            F.sum("qty").alias("volume"),
            F.count("*").alias("trade_count"),
            (F.sum(F.col("price") * F.col("qty")) / F.sum("qty")).alias("vwap"),
        )
        .select("symbol", F.col("window.start").alias("minute"), "open", "high", "low", "close", "volume",
                "trade_count", "vwap", F.current_timestamp().alias("_updated_at"))
    )


class BronzeSink:
    """foreachBatch sink. The late-event watermark is tracked on the driver
    (shortcut: resets on restart, so the first batch after a restart reports 0 late events)."""

    def __init__(self) -> None:
        self.max_trade_time: datetime | None = None

    def __call__(self, batch: DataFrame, batch_id: int) -> None:
        batch = batch.cache()
        now = F.current_timestamp()
        meta = [F.lit(f"stream:{batch_id}").alias("_run_id"), F.lit("binance").alias("_source"),
                now.alias("_ingested_at"), F.sha2("json", 256).alias("_payload_hash")]
        good = batch.where("_valid")
        cols = ["symbol", "trade_id", "price", "qty", "trade_time", "event_time", "is_buyer_maker",
                "kafka_partition", "kafka_offset"]
        good.select(*cols, *meta).writeTo("lake.bronze.crypto_trades").append()

        bad = batch.where(~F.col("_valid"))
        bad.select(F.lit("binance").alias("source"), F.lit("crypto_trades").alias("entity"),
                   F.lit("failed trade contract").alias("reason"), F.col("json").alias("_payload"),
                   *meta, F.current_date().alias("_logical_date")).writeTo("lake.bronze.quarantine").append()

        stats = good.agg(
            F.count("*").alias("events"), F.max("trade_time").alias("max_tt"), F.min("trade_time").alias("min_tt"),
            F.max(F.unix_timestamp(now) - F.unix_timestamp("event_time")).alias("lag"),
            F.sum(F.when(F.col("trade_time") < F.lit(self._watermark()), 1).otherwise(0)).alias("late"),
        ).first()
        parse_errors = bad.count()
        batch.unpersist()
        if stats["max_tt"] and (self.max_trade_time is None or stats["max_tt"] > self.max_trade_time):
            self.max_trade_time = stats["max_tt"]
        write_metrics(batch_id, "bronze_trades", stats["min_tt"], stats["events"], stats["late"] or 0,
                      parse_errors, stats["lag"])

    def _watermark(self) -> datetime:
        if self.max_trade_time is None:
            return datetime(1970, 1, 1)
        return self.max_trade_time - timedelta(minutes=2)


class EquitySink:
    """foreachBatch sink for delayed equity bars: Bronze append, Silver MERGE, metrics.
    Late = bar older than (newest bar seen - 2 min); a producer restart re-publishes recent history, which
    therefore shows up as late events (expected, and absorbed by the MERGE)."""

    def __init__(self) -> None:
        self.max_minute: datetime | None = None

    def __call__(self, batch: DataFrame, batch_id: int) -> None:
        batch = batch.cache()
        spark = batch.sparkSession
        now = F.current_timestamp()
        meta = [F.lit(f"stream:{batch_id}").alias("_run_id"), F.lit("yfinance").alias("_source"),
                now.alias("_ingested_at"), F.sha2("json", 256).alias("_payload_hash")]
        good = batch.where("_valid")
        bronze_cols = [*EQUITY_COLS, "kafka_partition", "kafka_offset"]
        good.select(*bronze_cols, *meta).writeTo("lake.bronze.equity_bars_1m").append()

        bad = batch.where(~F.col("_valid"))
        bad.select(F.lit("yfinance").alias("source"), F.lit("equity_bars_1m").alias("entity"),
                   F.lit("failed equity bar contract").alias("reason"), F.col("json").alias("_payload"),
                   *meta, F.current_date().alias("_logical_date")).writeTo("lake.bronze.quarantine").append()

        # Silver hash covers bar values only: a re-published bar (new published_at) is not a change.
        values_hash = F.sha2(F.concat_ws("|", *[F.col(c).cast("string") for c in ("open", "high", "low", "close",
                                                                                     "volume")]), 256)
        src = latest_per_key(
            good.select(*EQUITY_COLS, *meta[:3], values_hash.alias("_payload_hash")),
            ["ticker", "minute"], order_col="published_at",
        ).withColumn("_updated_at", now)
        src.createOrReplaceTempView("equity_src")
        spark.sql("""
            merge into lake.silver.equity_bars_1m t
            using equity_src s
            on t.ticker = s.ticker and t.minute = s.minute
            when matched and s._payload_hash <> t._payload_hash then update set *
            when not matched then insert *
        """)

        cutoff = (self.max_minute - timedelta(minutes=2)) if self.max_minute else datetime(1970, 1, 1)
        stats = good.agg(
            F.count("*").alias("events"), F.max("minute").alias("max_m"), F.min("minute").alias("min_m"),
            F.max(F.unix_timestamp(now) - F.unix_timestamp("minute") - 60).alias("lag"),
            F.sum(F.when(F.col("minute") < F.lit(cutoff), 1).otherwise(0)).alias("late"),
        ).first()
        parse_errors = bad.count()
        batch.unpersist()
        if stats["max_m"] and (self.max_minute is None or stats["max_m"] > self.max_minute):
            self.max_minute = stats["max_m"]
        if stats["events"] or parse_errors:
            write_metrics(batch_id, "equity_bars", stats["min_m"], stats["events"], stats["late"] or 0,
                          parse_errors, stats["lag"], topic=EQUITY_TOPIC)


def write_metrics(batch_id: int, query: str, window_start, events: int, late: int, errors: int, lag,
                  topic: str = TOPIC) -> None:
    try:  # fail-open: metrics must never stop the stream
        with psycopg.connect(settings().pg_dsn, connect_timeout=5, autocommit=True) as conn:
            conn.execute(
                """insert into ops.stream_metrics (batch_id, query_name, window_start, topic, events,
                       late_events, parse_errors, max_event_lag_seconds)
                   values (%s, %s, %s, %s, %s, %s, %s, %s)""",
                (batch_id, query, window_start, topic, events, late, errors, lag),
            )
    except Exception as exc:  # noqa: BLE001
        print(f"{datetime.now(timezone.utc).isoformat()} stream_metrics write failed: {type(exc).__name__}")


def main() -> None:
    s = settings()
    spark = spark_session("stream_market", {
        "spark.sql.shuffle.partitions": "4",
        # Binance uses fields that differ only by case (t/T, e/E, m/M).
        "spark.sql.caseSensitive": "true",
        "spark.hadoop.fs.s3a.endpoint": s.s3_endpoint,
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        "spark.hadoop.fs.s3a.access.key": s.s3_access_key,
        "spark.hadoop.fs.s3a.secret.key": s.s3_secret_key.get_secret_value(),
        "spark.hadoop.fs.s3a.endpoint.region": s.s3_region,
    })
    def kafka(topic: str) -> DataFrame:
        return (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", s.kafka_bootstrap)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")  # used only on first start; afterwards the checkpoint wins
            .option("maxOffsetsPerTrigger", "200000")
            .load()
        )

    trades = parse(kafka(TOPIC))
    (trades.writeStream.queryName("bronze_trades").foreachBatch(BronzeSink())
     .option("checkpointLocation", f"{CHECKPOINTS}/bronze_trades").trigger(processingTime=TRIGGER).start())
    (to_bars(trades.where("_valid")).writeStream.queryName("bars_1m").format("iceberg").outputMode("append")
     .option("checkpointLocation", f"{CHECKPOINTS}/bars_1m").trigger(processingTime=TRIGGER)
     .toTable("lake.silver.crypto_bars_1m"))
    (parse_equity(kafka(EQUITY_TOPIC)).writeStream.queryName("equity_bars").foreachBatch(EquitySink())
     .option("checkpointLocation", f"{CHECKPOINTS}/equity_bars").trigger(processingTime=TRIGGER).start())
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
