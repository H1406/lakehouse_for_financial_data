"""All Iceberg table schemas and partition specs, in one place. `create_all()` is idempotent."""

from pyiceberg.catalog import Catalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.transforms import BucketTransform, DayTransform, IdentityTransform
from pyiceberg.types import (
    BooleanType,
    DateType,
    DoubleType,
    IcebergType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestamptzType,
)

NAMESPACES = ("bronze", "silver")

S, D, F, L, INT, B = StringType(), DateType(), DoubleType(), LongType(), IntegerType(), BooleanType()
TS = TimestamptzType()

META = [("_run_id", S), ("_source", S), ("_ingested_at", TS), ("_payload_hash", S)]
# Batch Bronze tables are partitioned by the run's logical date so a rerun overwrites exactly its own partition.
BATCH_META = [*META, ("_logical_date", D)]
SILVER_META = [*META, ("_updated_at", TS)]

# yfinance daily: OHLC are split-adjusted; adj_close is split + dividend adjusted.
PRICE_COLS = [
    ("ticker", S), ("trade_date", D), ("open", F), ("high", F), ("low", F), ("close", F), ("adj_close", F),
    ("volume", L), ("div_cash", F), ("split_factor", F),
]
FACT_COLS = [
    ("cik", S), ("ticker", S), ("taxonomy", S), ("concept", S), ("unit", S), ("period_start", D),
    ("period_end", D), ("value", F), ("accession_no", S), ("fy", INT), ("fp", S), ("form", S),
    ("filed_date", D), ("frame", S),
]
TRADE_COLS = [
    ("symbol", S), ("trade_id", L), ("price", F), ("qty", F), ("trade_time", TS), ("event_time", TS),
    ("is_buyer_maker", B),
]
BAR_COLS = [
    ("symbol", S), ("minute", TS), ("open", F), ("high", F), ("low", F), ("close", F), ("volume", F),
    ("trade_count", L), ("vwap", F),
]

# Delayed (>= 10 min) 1-minute equity bars from the yfinance stream producer.
EQUITY_BAR_COLS = [
    ("ticker", S), ("minute", TS), ("open", F), ("high", F), ("low", F), ("close", F), ("volume", L),
    ("published_at", TS),
]

# name -> (columns, [(source column, transform, partition name)])
TABLES: dict[str, tuple[list[tuple[str, IcebergType]], list[tuple]]] = {
    "bronze.prices_eod_raw": (
        [*PRICE_COLS, ("_raw_line", S), *BATCH_META],
        [("_logical_date", IdentityTransform(), "_logical_date")],
    ),
    "bronze.sec_companyfacts_raw": (
        [("cik", S), ("ticker", S), ("_payload", S), ("payload_bytes", L), *BATCH_META],
        [("_logical_date", IdentityTransform(), "_logical_date")],
    ),
    "bronze.crypto_trades": (
        [*TRADE_COLS, ("kafka_partition", INT), ("kafka_offset", L), *META],
        [("trade_time", DayTransform(), "trade_day"), ("symbol", BucketTransform(4), "symbol_bucket")],
    ),
    "bronze.equity_bars_1m": (
        [*EQUITY_BAR_COLS, ("kafka_partition", INT), ("kafka_offset", L), *META],
        [("minute", DayTransform(), "minute_day")],
    ),
    "bronze.quarantine": (
        [("source", S), ("entity", S), ("reason", S), ("_payload", S), *BATCH_META],
        [("_logical_date", IdentityTransform(), "_logical_date")],
    ),
    # Silver prices/facts are small (tens/hundreds of thousands of rows): unpartitioned avoids tiny files.
    "silver.prices_eod": ([*PRICE_COLS, *SILVER_META], []),
    "silver.financial_facts": ([*FACT_COLS, *SILVER_META], []),
    "silver.crypto_trades": ([*TRADE_COLS, *SILVER_META], [("trade_time", DayTransform(), "trade_day")]),
    "silver.crypto_bars_1m": ([*BAR_COLS, ("_updated_at", TS)], [("minute", DayTransform(), "minute_day")]),
    "silver.equity_bars_1m": ([*EQUITY_BAR_COLS, *SILVER_META], [("minute", DayTransform(), "minute_day")]),
}


def build(columns: list[tuple[str, IcebergType]], parts: list[tuple]) -> tuple[Schema, PartitionSpec]:
    schema = Schema(*[NestedField(i, name, typ, required=False) for i, (name, typ) in enumerate(columns, 1)])
    ids = {name: i for i, (name, _) in enumerate(columns, 1)}
    spec = PartitionSpec(
        *[PartitionField(ids[col], 1000 + n, tr, name) for n, (col, tr, name) in enumerate(parts)]
    )
    return schema, spec


def sync_schema(table: Table, wanted: Schema) -> tuple[list[str], list[str]]:
    """Schema evolution for an existing table: drop columns no longer defined, add new ones.
    Metadata-only in Iceberg (column ids are never reused). Partition specs are not changed here."""
    current = {f.name for f in table.schema().fields}
    drop = sorted(current - {f.name for f in wanted.fields})
    add = [f for f in wanted.fields if f.name not in current]
    if drop or add:
        with table.update_schema() as u:
            for name in drop:
                u.delete_column(name)
            for f in add:
                u.add_column(f.name, f.field_type)
    return drop, [f.name for f in add]


def create_all(cat: Catalog) -> list[str]:
    for ns in NAMESPACES:
        cat.create_namespace_if_not_exists(ns)
    report = []
    for name, (cols, parts) in TABLES.items():
        schema, spec = build(cols, parts)
        if cat.table_exists(name):
            drop, add = sync_schema(cat.load_table(name), schema)
            report.append(f"{name}: exists" + (f", dropped {drop}" if drop else "") + (f", added {add}" if add else ""))
        else:
            cat.create_table(name, schema=schema, partition_spec=spec, properties={"format-version": "2"})
            report.append(f"{name}: created")
    return report
