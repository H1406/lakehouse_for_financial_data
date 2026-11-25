"""Idempotent Bronze writes with PyIceberg.

Each batch write replaces exactly its own slice — (_source, _logical_date[, extra keys]) — in one atomic
Iceberg commit (delete + append in a single transaction), so rerunning a logical date never appends twice.
"""

import hashlib
from datetime import UTC, date, datetime

import pyarrow as pa
from pyiceberg.expressions import And, BooleanExpression, EqualTo, In
from pyiceberg.io.pyarrow import schema_to_pyarrow

from lakehouse.iceberg.catalog import catalog


def payload_hash(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def slice_filter(source: str, logical_date: date, **in_filters: list[str]) -> BooleanExpression:
    expr: BooleanExpression = And(EqualTo("_source", source), EqualTo("_logical_date", logical_date.isoformat()))
    for col, values in in_filters.items():
        expr = And(expr, In(col, values))
    return expr


def overwrite_slice(table_name: str, rows: list[dict], *, source: str, logical_date: date, run_id: str,
                    payload_col: str, **in_filters: list[str]) -> int:
    """Replace the (source, logical_date[, in_filters]) slice of `table_name` with `rows`."""
    table = catalog().load_table(table_name)
    now = datetime.now(UTC)
    for r in rows:
        r.update(_run_id=run_id, _source=source, _ingested_at=now, _logical_date=logical_date,
                 _payload_hash=payload_hash(r[payload_col]))
    arrow_schema = schema_to_pyarrow(table.schema(), include_field_ids=False)
    data = pa.Table.from_pylist(rows, schema=arrow_schema)
    with table.transaction() as tx:
        tx.delete(slice_filter(source, logical_date, **in_filters))
        if rows:
            tx.append(data)
    return len(rows)


def write_quarantine(rows: list[dict], *, source: str, entity: str | None, logical_date: date, run_id: str) -> int:
    """Quarantine slice = (source, logical_date[, entity]). Rows: {entity, reason, _payload}."""
    for r in rows:
        r["source"] = source
    extra = {"entity": [entity]} if entity else {}
    return overwrite_slice("bronze.quarantine", rows, source=source, logical_date=logical_date,
                           run_id=run_id, payload_col="_payload", **extra)


def slice_keys(table_name: str, key_cols: list[str], *, source: str, logical_date: date,
               **in_filters: list[str]) -> list[tuple]:
    """Key tuples of one slice (only key columns are read, so large payload columns are not scanned)."""
    table = catalog().load_table(table_name)
    arrow = table.scan(row_filter=slice_filter(source, logical_date, **in_filters),
                       selected_fields=tuple(key_cols)).to_arrow()
    return list(zip(*(arrow.column(c).to_pylist() for c in key_cols), strict=True))
