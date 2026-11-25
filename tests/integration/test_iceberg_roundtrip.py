"""Day-1 smoke test: PyIceberg creates a table -> Spark appends -> PyIceberg reads it back.
Runs inside the airflow container against the live stack (`make smoke`)."""

import uuid

import pyarrow as pa
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType

from lakehouse.iceberg.catalog import catalog
from spark_jobs.common import spark_session


def test_pyiceberg_spark_roundtrip():
    cat = catalog()
    cat.create_namespace_if_not_exists("smoke")
    name = f"smoke.t_{uuid.uuid4().hex[:8]}"
    table = cat.create_table(
        name, schema=Schema(NestedField(1, "id", LongType()), NestedField(2, "who", StringType()))
    )
    try:
        table.append(pa.table({"id": pa.array([1], pa.int64()), "who": ["pyiceberg"]}))
        spark = spark_session("smoke")
        spark.sql(f"insert into lake.{name} values (2, 'spark')")
        rows = sorted(cat.load_table(name).scan().to_arrow().to_pylist(), key=lambda r: r["id"])
        assert rows == [{"id": 1, "who": "pyiceberg"}, {"id": 2, "who": "spark"}]
    finally:
        cat.drop_table(name)
