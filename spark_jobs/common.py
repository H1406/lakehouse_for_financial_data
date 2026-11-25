"""Shared SparkSession: catalog `lake` = Iceberg REST catalog on SeaweedFS S3, AQE on.

JARs (iceberg-spark-runtime, iceberg-aws-bundle, Kafka connector) are baked into the images.
"""

import argparse
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def spark_session(app: str, extra: dict[str, str] | None = None) -> SparkSession:
    endpoint = os.getenv("S3_ENDPOINT", "http://seaweedfs:8333")
    builder = (
        SparkSession.builder.appName(app)
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "2g"))
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lake.type", "rest")
        .config("spark.sql.catalog.lake.uri", os.getenv("ICEBERG_REST_URI", "http://iceberg-rest:8181"))
        .config("spark.sql.catalog.lake.warehouse", "s3://lake/warehouse")
        .config("spark.sql.catalog.lake.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.lake.s3.endpoint", endpoint)
        .config("spark.sql.catalog.lake.s3.path-style-access", "true")
        .config("spark.sql.catalog.lake.client.region", os.getenv("S3_REGION", "us-east-1"))
        .config("spark.sql.defaultCatalog", "lake")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        # Laptop-sized default; AQE coalesces further. A cluster would size this to the data.
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "16"))
        .config("spark.ui.showConsoleProgress", "false")
    )
    for k, v in (extra or {}).items():
        builder = builder.config(k, v)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def batch_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--logical-date", required=True)
    p.add_argument("--run-id", required=True)
    p.add_argument("--source", required=True)
    return p.parse_args()


def latest_per_key(df: DataFrame, key: list[str], order_col: str = "_ingested_at") -> DataFrame:
    """One row per key: the most recent by `order_col` (ties broken by payload hash for determinism)."""
    w = Window.partitionBy(*key).orderBy(F.col(order_col).desc(), F.col("_payload_hash"))
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")
