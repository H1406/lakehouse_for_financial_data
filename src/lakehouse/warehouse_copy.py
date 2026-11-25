"""Incremental copy of Iceberg Silver tables into Postgres `lake_silver` (upsert by key).

Reads only rows with `_updated_at` past the stored watermark (minus a small overlap, which the upsert
makes harmless), COPYs them into a temp table, then INSERT ... ON CONFLICT DO UPDATE.
Uses PyIceberg rather than Spark: the volumes are small and this avoids a JVM start and a JDBC driver.
"""

from datetime import UTC, datetime, timedelta

from psycopg import sql
from pyiceberg.expressions import AlwaysTrue, GreaterThan

from lakehouse.iceberg.catalog import catalog
from lakehouse.ops import connect, get_watermark, instrumented, record_reconciliation, set_watermark

KEYS = {
    "prices_eod": ["ticker", "trade_date"],
    "financial_facts": ["cik", "concept", "unit", "period_start", "period_end", "accession_no"],
    "crypto_bars_1m": ["symbol", "minute"],
    "equity_bars_1m": ["ticker", "minute"],
}
CONFLICT_TARGET = {"financial_facts": sql.SQL("on constraint financial_facts_key")}
OVERLAP = timedelta(minutes=5)


def _pg_columns(conn, table: str) -> list[str]:
    rows = conn.execute(
        "select column_name from information_schema.columns where table_schema = 'lake_silver' "
        "and table_name = %s order by ordinal_position", (table,)
    ).fetchall()
    return [r[0] for r in rows]


def upsert_sql(table: str, cols: list[str]) -> sql.Composed:
    keys = KEYS[table]
    target = CONFLICT_TARGET.get(table) or sql.SQL("({})").format(sql.SQL(", ").join(map(sql.Identifier, keys)))
    updates = [c for c in cols if c not in keys]
    return sql.SQL(
        "insert into lake_silver.{t} ({cols}) select {cols} from tmp_copy on conflict {target} do update set {set}"
    ).format(
        t=sql.Identifier(table),
        cols=sql.SQL(", ").join(map(sql.Identifier, cols)),
        target=target,
        set=sql.SQL(", ").join(sql.SQL("{c} = excluded.{c}").format(c=sql.Identifier(c)) for c in updates),
    )


@instrumented(source="lake_silver", entity="copy")
def copy_table(*, table: str, run_id: str, logical_date=None) -> dict:
    wm = get_watermark("lake_silver", table)
    row_filter = (GreaterThan("_updated_at", (datetime.fromisoformat(wm) - OVERLAP).isoformat())
                  if wm else AlwaysTrue())
    arrow = catalog().load_table(f"silver.{table}").scan(row_filter=row_filter).to_arrow()
    if arrow.num_rows == 0:
        return {"rows_read": 0, "rows_written": 0}

    with connect() as conn:
        cols = _pg_columns(conn, table)
        arrow = arrow.select(cols)
        with conn.transaction():
            conn.execute(sql.SQL("create temp table tmp_copy (like lake_silver.{t}) on commit drop").format(
                t=sql.Identifier(table)))
            with conn.cursor().copy(sql.SQL("copy tmp_copy ({}) from stdin").format(
                    sql.SQL(", ").join(map(sql.Identifier, cols)))) as cp:
                for batch in arrow.to_batches(max_chunksize=50_000):
                    for row in zip(*(batch.column(c).to_pylist() for c in cols), strict=True):
                        cp.write_row(row)
            written = conn.execute(upsert_sql(table, cols)).rowcount

    new_wm = max(arrow.column("_updated_at").to_pylist())
    set_watermark("lake_silver", table, new_wm.astimezone(UTC).isoformat())
    record_reconciliation(run_id=run_id, source="lake_silver", entity=table, logical_date=logical_date,
                          layer_from="silver", layer_to="postgres", count_from=arrow.num_rows, count_to=written)
    return {"rows_read": arrow.num_rows, "rows_written": written}
