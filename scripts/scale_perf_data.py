"""Build the scaled `perf` namespace for docs/PERFORMANCE.md (real data is too small for tuning to matter).

    spark-submit scripts/scale_perf_data.py --copies 20

perf.sec_companyfacts_raw  13 real EDGAR payloads x COPIES under synthetic CIKs, spread over 10 daily
                           partitions (2026-01-01..10), plus:
                           - a "mega" company: JPM's payload with every fact repeated 10x (one huge row -> skew)
                           - partition 2026-01-11: a re-land of day 10 with ~1 in 20 values changed (restatements)
perf.financial_facts       Silver target, pre-loaded with days 1-10 (the state every benchmark run starts from)
"""

import argparse
import json
from datetime import date

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from spark_jobs.bronze_to_silver_facts import KEY, explode_facts
from spark_jobs.common import latest_per_key, spark_session

DAYS = 10
RELAND = date(2026, 1, 11)


MEGA_FACTOR = 10
# Each units array holds flat fact objects (no nested brackets), so a regex can repeat its contents in Spark:
# ":[{...},{...}]"  ->  ":[{...},{...},{...},{...}, ...]"  (MEGA_FACTOR times). No Python-side JSON parsing.
MEGA_PATTERN = r"(:\[)([^\[\]]*)(\])"
MEGA_REPLACEMENT = "$1" + ",".join(["$2"] * MEGA_FACTOR) + "$3"


def main(copies: int) -> None:
    spark = spark_session("scale_perf_data")
    spark.sql("create namespace if not exists lake.perf")
    bronze = spark.table("lake.bronze.sec_companyfacts_raw").where("_source = 'sec_edgar'")
    # Copies are made inside Spark (cross join), never collected to Python: payloads total ~1 GB.
    n = F.col("copy") * 13 + F.col("rn")
    numbered = bronze.withColumn("rn", F.row_number().over(Window.orderBy("cik")))
    scaled = (numbered.crossJoin(spark.range(copies).withColumnRenamed("id", "copy"))
              .withColumn("cik", F.format_string("9%09d", n))
              .withColumn("ticker", F.format_string("SYN%04d", n))
              .withColumn("_logical_date", F.date_add(F.lit(date(2026, 1, 1)), (n % DAYS).cast("int")))
              .withColumn("_run_id", F.lit("perf")))
    cols = bronze.columns
    mega_df = (bronze.where("ticker = 'JPM'").select(*cols)
               .withColumn("_payload", F.regexp_replace("_payload", MEGA_PATTERN, MEGA_REPLACEMENT))
               .withColumn("payload_bytes", F.length("_payload").cast("long"))
               .withColumn("cik", F.lit("9999999999")).withColumn("ticker", F.lit("MEGA"))
               .withColumn("_run_id", F.lit("perf")).withColumn("_logical_date", F.lit(date(2026, 1, DAYS))))
    days = scaled.select(*cols).unionByName(mega_df)
    # Re-land of day 10 with restated values: every value ending in 7 gets one more digit (~1 in 10 change).
    reland = (days.where(F.col("_logical_date") == F.lit(date(2026, 1, DAYS)))
              .withColumn("_payload", F.regexp_replace("_payload", r'("val":\s?-?\d*7)([,}])', "$11$2"))
              .withColumn("_logical_date", F.lit(RELAND)).withColumn("_payload_hash", F.lit("reland")))
    spark.sql("drop table if exists lake.perf.sec_companyfacts_raw")
    days.unionByName(reland).writeTo("lake.perf.sec_companyfacts_raw").partitionedBy(F.col("_logical_date")).create()

    base = spark.table("lake.perf.sec_companyfacts_raw").where(F.col("_logical_date") < F.lit(RELAND))
    facts = latest_per_key(explode_facts(base), KEY).withColumn("_updated_at", F.current_timestamp())
    spark.sql("drop table if exists lake.perf.financial_facts")
    facts.select(*spark.table("lake.silver.financial_facts").columns).writeTo("lake.perf.financial_facts").create()
    b = spark.table("lake.perf.sec_companyfacts_raw")
    print(json.dumps({
        "bronze_rows": b.count(), "reland_rows": b.where(F.col("_logical_date") == F.lit(RELAND)).count(),
        "mega_payload_mb": round(b.where("ticker = 'MEGA'").first()["payload_bytes"] / 1e6, 1),
        "target_rows": spark.table("lake.perf.financial_facts").count(),
        "target_files": spark.sql("select count(*) c from lake.perf.financial_facts.files").first()["c"]}))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--copies", type=int, default=20)
    main(p.parse_args().copies)
