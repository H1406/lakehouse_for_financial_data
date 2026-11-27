from agents.qa.sql_guard import ALLOWED_TABLES
from lakehouse.lineage import QA_SQL_TABLES, attach_reconciliation, build_graph, dbt_graph

MANIFEST = {
    "nodes": {
        "model.lakehouse.stg_prices_eod": {"name": "stg_prices_eod", "schema": "staging", "description": "typed"},
        "model.lakehouse.int_daily_returns": {"name": "int_daily_returns", "schema": "intermediate"},
        "seed.lakehouse.universe": {"name": "universe", "schema": "staging"},
        "test.lakehouse.not_null_x": {"name": "not_null_x", "schema": "staging"},
    },
    "sources": {"source.lakehouse.lake_silver.prices_eod": {"identifier": "prices_eod", "schema": "lake_silver"}},
    "parent_map": {
        "model.lakehouse.stg_prices_eod": ["source.lakehouse.lake_silver.prices_eod"],
        "model.lakehouse.int_daily_returns": ["model.lakehouse.stg_prices_eod"],
        "test.lakehouse.not_null_x": ["model.lakehouse.stg_prices_eod"],
    },
}


def test_dbt_graph_uses_relations_and_skips_tests():
    nodes, edges = dbt_graph(MANIFEST)
    ids = {n["id"] for n in nodes}
    assert ids == {"pg:staging.stg_prices_eod", "pg:intermediate.int_daily_returns", "pg:staging.universe",
                   "pg:lake_silver.prices_eod"}
    assert {(e["from"], e["to"]) for e in edges} == {
        ("pg:lake_silver.prices_eod", "pg:staging.stg_prices_eod"),
        ("pg:staging.stg_prices_eod", "pg:intermediate.int_daily_returns")}


def test_build_graph_joins_upstream_and_dbt_and_every_edge_end_exists():
    g = build_graph(MANIFEST)
    ids = {n["id"] for n in g["nodes"]}
    assert all(e["from"] in ids and e["to"] in ids for e in g["edges"])
    assert ("iceberg:silver.prices_eod", "pg:lake_silver.prices_eod") in {(e["from"], e["to"]) for e in g["edges"]}
    assert all(n["layer"] in g["layers"] for n in g["nodes"]) and not g["warnings"]


def test_missing_manifest_is_a_warning_not_an_error():
    g = build_graph(None)
    assert g["warnings"] and not any(e["kind"] == "dbt" for e in g["edges"])


def test_reconciliation_picks_newest_row_across_sources():
    g = build_graph(None)
    latest = [
        {"source": "yfinance", "entity": "prices_eod", "layer_from": "bronze", "layer_to": "silver",
         "status": "ok", "checked_at": "2026-11-26T10:00:00", "failed_7d": 1},
        {"source": "yfinance_backfill", "entity": "prices_eod", "layer_from": "bronze", "layer_to": "silver",
         "status": "mismatch", "checked_at": "2026-11-25T10:00:00", "failed_7d": 0},
    ]
    attach_reconciliation(g["edges"], latest)
    (edge,) = [e for e in g["edges"] if e["to"] == "iceberg:silver.prices_eod"]
    assert edge["check"]["status"] == "ok" and edge["check"]["failed_7d"] == 1


def test_qa_allowlist_matches_sql_guard():
    assert set(QA_SQL_TABLES) == ALLOWED_TABLES
