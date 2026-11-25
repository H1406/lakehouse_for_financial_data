"""Bronze companyfacts payloads -> silver.financial_facts.

Parse the JSON with an explicit schema, explode taxonomy -> concept -> unit -> fact using built-in
functions only (no Python UDF), dedup, then MERGE INTO on
(cik, concept, unit, period_start, period_end, accession_no). period_start is null for instant facts,
so the merge condition uses null-safe equality (<=>).
"""

from datetime import date
from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from lakehouse.ops import instrumented, record_reconciliation
from spark_jobs.common import batch_args, latest_per_key, spark_session

KEY = ["cik", "concept", "unit", "period_start", "period_end", "accession_no"]
MIN_PERIOD_END = "2019-01-01"
TAXONOMY = "us-gaap"  # shortcut: dei/ifrs facts are skipped; add them (and taxonomy to the key) if needed

FACT = T.StructType([
    T.StructField("start", T.StringType()), T.StructField("end", T.StringType()),
    T.StructField("val", T.DoubleType()), T.StructField("accn", T.StringType()),
    T.StructField("fy", T.IntegerType()), T.StructField("fp", T.StringType()),
    T.StructField("form", T.StringType()), T.StructField("filed", T.StringType()),
    T.StructField("frame", T.StringType()),
])
CONCEPT = T.StructType([
    T.StructField("label", T.StringType()),
    T.StructField("units", T.MapType(T.StringType(), T.ArrayType(FACT))),
])
PAYLOAD = T.StructType([
    T.StructField("cik", T.LongType()),
    T.StructField("facts", T.MapType(T.StringType(), T.MapType(T.StringType(), CONCEPT))),
])


def explode_facts(bronze: DataFrame) -> DataFrame:
    meta = ["cik", "ticker", "_run_id", "_source", "_ingested_at"]
    df = bronze.select(*meta, F.from_json("_payload", PAYLOAD).alias("j"))
    df = df.select(*meta, F.explode("j.facts").alias("taxonomy", "concepts")).where(F.col("taxonomy") == TAXONOMY)
    df = df.select(*meta, "taxonomy", F.explode("concepts").alias("concept", "c"))
    df = df.select(*meta, "taxonomy", "concept", F.explode("c.units").alias("unit", "facts"))
    df = df.select(*meta, "taxonomy", "concept", "unit", F.explode("facts").alias("f"))
    df = df.select(
        *meta, "taxonomy", "concept", "unit",
        F.to_date("f.start").alias("period_start"), F.to_date("f.end").alias("period_end"),
        F.col("f.val").alias("value"), F.col("f.accn").alias("accession_no"), F.col("f.fy").alias("fy"),
        F.col("f.fp").alias("fp"), F.col("f.form").alias("form"), F.to_date("f.filed").alias("filed_date"),
        F.col("f.frame").alias("frame"),
    ).where(F.col("period_end") >= F.lit(MIN_PERIOD_END))
    # Row-level hash so a re-landed but unchanged fact does not count as an update.
    return df.withColumn(
        "_payload_hash",
        F.sha2(F.concat_ws("|", *[F.col(c).cast("string") for c in ("value", "fy", "fp", "form", "filed_date",
                                                                       "frame")]), 256),
    )


def null_safe_join(left: DataFrame, right: DataFrame, how: str) -> DataFrame:
    cond = reduce(lambda a, b: a & b, [left[c].eqNullSafe(right[c]) for c in KEY])
    return left.join(right, cond, how)


@instrumented(source="sec_edgar", entity="financial_facts")
def run(spark: SparkSession, *, logical_date: date, run_id: str, source: str) -> dict:
    bronze = spark.table("lake.bronze.sec_companyfacts_raw").where(
        (F.col("_source") == source) & (F.col("_logical_date") == F.lit(logical_date))
    )
    facts = explode_facts(bronze).cache()
    src = latest_per_key(facts, KEY).withColumn("_updated_at", F.current_timestamp()).cache()
    src.createOrReplaceTempView("src")
    on = " and ".join(f"t.{c} <=> s.{c}" for c in KEY)
    spark.sql(f"""
        merge into lake.silver.financial_facts t
        using src s
        on {on}
        when matched and s._ingested_at >= t._ingested_at and s._payload_hash <> t._payload_hash
          then update set *
        when not matched then insert *
    """)

    exploded, distinct_keys = facts.count(), src.count()
    silver = spark.table("lake.silver.financial_facts")
    silver_rows = null_safe_join(silver, src.select(*KEY), "left_semi").count()
    # Repeated facts inside one EDGAR payload are a source quirk (deduped above), not a load error;
    # `duplicates` counts duplicate Bronze payloads for the same CIK in this slice.
    payloads = bronze.select("cik")
    status = record_reconciliation(
        run_id=run_id, source=source, entity="financial_facts", logical_date=logical_date,
        layer_from="bronze", layer_to="silver", count_from=distinct_keys, count_to=silver_rows,
        missing=distinct_keys - silver_rows, duplicates=payloads.count() - payloads.distinct().count(),
    )
    return {"rows_read": exploded, "rows_written": distinct_keys, "status": status}


if __name__ == "__main__":
    args = batch_args()
    run(spark_session("bronze_to_silver_facts"), logical_date=date.fromisoformat(args.logical_date),
        run_id=args.run_id, source=args.source)
