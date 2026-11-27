"""Table-level lineage graph for the data browser's flow map (pure functions, no I/O).

Two sources of truth, so the map is not a hand-drawn picture:
- dbt hops (lake_silver -> staging -> intermediate -> marts) come from dbt's own `target/manifest.json`
  (`parent_map`), so they change when the models change.
- Hops dbt can't see (APIs -> Bronze -> Silver -> Postgres, the streams, the agents) are declared in
  UPSTREAM below. Each mirrors a job in spark_jobs/, src/lakehouse/ or agents/; `via` names it.
Batch hops carry the `ops.reconciliation` keys they write, so the map can colour them by the latest check.
"""

from collections.abc import Iterable

# Column order on the map (left -> right).
LAYERS = ("source", "landing", "bronze", "silver", "lake_silver", "staging", "intermediate", "marts", "agent",
          "consumer")

# id -> (label, store, layer, description)
STATIC_NODES: dict[str, tuple[str, str, str, str]] = {
    "ext:yfinance_daily": ("yfinance daily", "external", "source", "Daily OHLCV per ticker (Airflow prices_eod)"),
    "ext:sec_edgar": ("SEC EDGAR", "external", "source", "companyfacts JSON per company (sec_facts_weekly)"),
    "ext:binance_ws": ("Binance trades WS", "external", "source", "Live trade WebSocket (streaming/producer.py)"),
    "ext:yfinance_1m": ("yfinance 1-min bars", "external", "source",
                        "Delayed >= 10 min, polled every 60 s (streaming/equity_producer.py)"),
    "ext:runbooks": ("runbooks/*.md", "external", "source", "Markdown runbooks in the repo"),
    "file:raw": ("data/raw", "local", "landing", "Staged raw API responses between extract and land"),
    "kafka:crypto_trades": ("crypto_trades", "kafka", "landing", "Redpanda topic, 6 partitions, key = symbol"),
    "kafka:equity_bars_1m": ("equity_bars_1m", "kafka", "landing", "Redpanda topic, 6 partitions, key = ticker"),
    "file:fact_sheets": ("data/fact_sheets", "local", "agent", "Rendered fact-sheet Markdown files"),
    "app:grafana": ("Grafana", "app", "consumer", "Dashboard + alert rules (scripts/build_dashboard.py)"),
    "app:qa": ("QA agent", "app", "consumer", "Hybrid search over fact sheets + read-only NL-SQL on marts"),
    "app:triage": ("Triage agent", "app", "consumer", "Reads ops + runbooks, writes a diagnosis only"),
}

ICEBERG_TABLES = {
    "bronze": ("prices_eod_raw", "sec_companyfacts_raw", "crypto_trades", "equity_bars_1m", "quarantine"),
    "silver": ("prices_eod", "financial_facts", "crypto_trades", "crypto_bars_1m", "equity_bars_1m"),
}
PG_FIXED = {  # Postgres tables outside dbt's graph that appear on the map, by layer
    "marts": ("ops.pipeline_run", "ops.reconciliation", "ops.stream_metrics", "ops.dbt_result", "ops.incident"),
    "agent": ("agent.documents", "agent.chunks", "agent.run_log"),
}

# Kept equal to agents/qa/sql_guard.py ALLOWED_TABLES (a unit test checks it).
QA_SQL_TABLES = ("dim_company", "fct_rolling_volatility", "fct_drawdown", "fct_correlation_60d",
                 "fct_fundamentals_asof", "fct_intraday_volatility", "mart_fact_sheet_inputs")
GRAFANA_TABLES = ("marts.fct_rolling_volatility", "marts.fct_drawdown", "marts.fct_correlation_60d",
                  "marts.fct_intraday_volatility", "ops.pipeline_run", "ops.reconciliation", "ops.stream_metrics",
                  "ops.dbt_result")
TRIAGE_TABLES = ("ops.incident", "ops.pipeline_run", "ops.reconciliation", "ops.stream_metrics", "ops.dbt_result")

YF = ("yfinance", "yfinance_backfill")
# (from, to, kind, via, reconciliation keys [(source, entity, layer_from, layer_to)])
UPSTREAM: list[tuple[str, str, str, str, list[tuple[str, str, str, str]]]] = [
    ("ext:yfinance_daily", "file:raw", "batch", "prices_eod DAG: extract", []),
    ("ext:sec_edgar", "file:raw", "batch", "sec_facts_weekly DAG: extract", []),
    ("file:raw", "iceberg:bronze.prices_eod_raw", "batch", "validate_and_land_bronze (dynamic partition overwrite)",
     [(s, "prices_eod", "source", "bronze") for s in YF]),
    ("file:raw", "iceberg:bronze.sec_companyfacts_raw", "batch", "validate_and_land_bronze",
     [("sec_edgar", "companyfacts", "source", "bronze")]),
    ("file:raw", "iceberg:bronze.quarantine", "batch", "pydantic contract failures", []),
    ("iceberg:bronze.prices_eod_raw", "iceberg:silver.prices_eod", "batch",
     "spark_jobs/bronze_to_silver_prices.py MERGE",
     [(s, "prices_eod", "bronze", "silver") for s in YF]),
    ("iceberg:bronze.sec_companyfacts_raw", "iceberg:silver.financial_facts", "batch",
     "spark_jobs/bronze_to_silver_facts.py explode + MERGE", [("sec_edgar", "financial_facts", "bronze", "silver")]),
    ("ext:binance_ws", "kafka:crypto_trades", "stream", "streaming/producer.py (acks=all, idempotent)", []),
    ("ext:yfinance_1m", "kafka:equity_bars_1m", "stream", "streaming/equity_producer.py", []),
    ("kafka:crypto_trades", "iceberg:bronze.crypto_trades", "stream", "stream_market.py query A (foreachBatch)", []),
    ("kafka:crypto_trades", "iceberg:bronze.quarantine", "stream", "stream_market.py query A: bad rows", []),
    ("kafka:crypto_trades", "iceberg:silver.crypto_bars_1m", "stream",
     "stream_market.py query B: 2-min watermark, 1-min window", []),
    ("kafka:equity_bars_1m", "iceberg:bronze.equity_bars_1m", "stream", "stream_market.py query C", []),
    ("kafka:equity_bars_1m", "iceberg:bronze.quarantine", "stream", "stream_market.py query C: bad rows", []),
    ("kafka:equity_bars_1m", "iceberg:silver.equity_bars_1m", "stream", "stream_market.py query C: MERGE", []),
    ("iceberg:bronze.crypto_trades", "iceberg:silver.crypto_trades", "batch",
     "spark_jobs/silver_crypto_trades.py dedup MERGE", [("binance", "crypto_trades", "bronze", "silver")]),
    *[(f"iceberg:silver.{t}", f"pg:lake_silver.{t}", "batch", "warehouse_copy.py incremental upsert",
       [("lake_silver", t, "silver", "postgres")])
      for t in ("prices_eod", "financial_facts", "equity_bars_1m", "crypto_bars_1m")],
    ("pg:marts.mart_fact_sheet_inputs", "file:fact_sheets", "batch", "fact_sheets DAG: render", []),
    ("pg:marts.mart_fact_sheet_inputs", "pg:agent.documents", "batch", "refresh_index (re-embed changed only)", []),
    ("ext:runbooks", "pg:agent.documents", "batch", "refresh_index", []),
    ("pg:agent.documents", "pg:agent.chunks", "batch", "chunk + all-MiniLM-L6-v2 embedding (pgvector)", []),
    ("pg:agent.chunks", "app:qa", "read", "search_fact_sheets: hybrid vector + full-text, RRF", []),
    ("pg:agent.chunks", "app:triage", "read", "search_runbooks: hybrid search", []),
    *[(f"pg:marts.{t}", "app:qa", "read", "run_sql as agent_ro (allowlist, row limit, timeout)", [])
      for t in QA_SQL_TABLES],
    *[(f"pg:{t}", "app:grafana", "read", "Grafana panels / alert rules as grafana_ro", []) for t in GRAFANA_TABLES],
    *[(f"pg:{t}", "app:triage", "read", "triage tools as agent_rw (SELECT on ops)", []) for t in TRIAGE_TABLES],
    ("app:qa", "pg:agent.run_log", "write", "every prompt, reply and tool call", []),
    ("app:triage", "pg:agent.run_log", "write", "every prompt, reply and tool call", []),
    ("app:triage", "pg:ops.incident", "write", "agent_diagnosis + triage_note_path only (column grant)", []),
]

DBT_LAYER = {"lake_silver": "lake_silver", "staging": "staging", "intermediate": "intermediate", "marts": "marts"}


def _node(nid: str, label: str, store: str, layer: str, description: str = "", **extra) -> dict:
    return {"id": nid, "label": label, "store": store, "layer": layer, "description": description, **extra}


def dbt_graph(manifest: dict) -> tuple[list[dict], list[dict]]:
    """dbt models, seeds and sources -> (nodes, edges) keyed by their Postgres relation `pg:schema.name`."""
    rel: dict[str, str] = {}
    nodes: list[dict] = []
    for uid, n in {**manifest.get("nodes", {}), **manifest.get("sources", {})}.items():
        rtype = uid.split(".", 1)[0]
        if rtype not in ("model", "seed", "source"):
            continue
        name = n.get("identifier") if rtype == "source" else (n.get("alias") or n["name"])
        schema = n["schema"]
        nid = f"pg:{schema}.{name}"
        rel[uid] = nid
        layer = DBT_LAYER.get(schema, "marts")
        nodes.append(_node(nid, name, "postgres", layer, (n.get("description") or "").strip(), dbt=rtype,
                           materialized=(n.get("config") or {}).get("materialized")))
    edges = [{"from": rel[p], "to": rel[c], "kind": "dbt", "via": "dbt " + c.split(".")[-1], "recon": []}
             for c, parents in manifest.get("parent_map", {}).items() if c in rel
             for p in parents if p in rel]
    return nodes, edges


def build_graph(manifest: dict | None) -> dict:
    """All nodes and edges of the map (no statistics). `manifest=None` drops the dbt part and says so."""
    nodes: dict[str, dict] = {nid: _node(nid, *spec) for nid, spec in STATIC_NODES.items()}
    for layer, tables in ICEBERG_TABLES.items():
        for t in tables:
            nodes[f"iceberg:{layer}.{t}"] = _node(f"iceberg:{layer}.{t}", t, "s3", layer, f"Iceberg {layer}.{t}")
    for layer, tables in PG_FIXED.items():
        for t in tables:
            nodes[f"pg:{t}"] = _node(f"pg:{t}", t, "postgres", layer, f"Postgres {t}")
    edges = [{"from": a, "to": b, "kind": k, "via": via, "recon": [list(r) for r in recon]}
             for a, b, k, via, recon in UPSTREAM]
    warnings = []
    if manifest:
        dnodes, dedges = dbt_graph(manifest)
        for n in dnodes:
            nodes.setdefault(n["id"], n).update({k: v for k, v in n.items() if v})
        edges += dedges
    else:
        warnings.append("dbt/target/manifest.json not found: dbt lineage hidden (run `make dbt-build`)")
    for e in edges:  # an edge may point at a table dbt created that isn't in the fixed lists
        for end in (e["from"], e["to"]):
            if end not in nodes and end.startswith("pg:"):
                schema, name = end[3:].split(".", 1)
                nodes[end] = _node(end, name, "postgres", DBT_LAYER.get(schema, "marts"))
    return {"layers": list(LAYERS), "nodes": list(nodes.values()), "edges": edges, "warnings": warnings}


def attach_reconciliation(edges: Iterable[dict], latest: Iterable[dict]) -> None:
    """Set each edge's `check` from the newest ops.reconciliation row among its keys (in place).

    `latest` = one row per (source, entity, layer_from, layer_to) with count_from, count_to, missing,
    duplicates, late, status, checked_at, failed_7d.
    """
    by_key = {(r["source"], r["entity"], r["layer_from"], r["layer_to"]): r for r in latest}
    for e in edges:
        rows = [by_key[tuple(k)] for k in e.get("recon", []) if tuple(k) in by_key]
        if rows:
            newest = max(rows, key=lambda r: str(r["checked_at"]))
            e["check"] = {**newest, "failed_7d": sum(int(r.get("failed_7d") or 0) for r in rows)}
