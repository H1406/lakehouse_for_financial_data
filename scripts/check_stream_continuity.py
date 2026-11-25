"""Stream restart test check. Binance trade ids are sequential per symbol, so for each symbol:
missing = (max_id - min_id + 1) - distinct ids. Also reports duplicate keys in Silver and replayed rows in Bronze.

Usage (inside the airflow container): python scripts/check_stream_continuity.py [--since ISO_TS]
"""

import argparse

import pyarrow.compute as pc
from pyiceberg.expressions import AlwaysTrue, GreaterThanOrEqual

from lakehouse.iceberg.catalog import catalog


def stats(table: str, since: str | None) -> dict[str, dict]:
    f = GreaterThanOrEqual("trade_time", since) if since else AlwaysTrue()
    t = catalog().load_table(table).scan(row_filter=f, selected_fields=("symbol", "trade_id")).to_arrow()
    out = {}
    for sym in pc.unique(t["symbol"]).to_pylist():
        ids = t.filter(pc.equal(t["symbol"], sym))["trade_id"]
        distinct = len(pc.unique(ids))
        lo, hi = pc.min(ids).as_py(), pc.max(ids).as_py()
        out[sym] = {"rows": len(ids), "distinct": distinct, "duplicates": len(ids) - distinct,
                    "missing": (hi - lo + 1) - distinct}
    return out


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--since")
    a = p.parse_args()
    for table in ("bronze.crypto_trades", "silver.crypto_trades"):
        for sym, s in sorted(stats(table, a.since).items()):
            print(f"{table:22} {sym:8} {s}")
