"""Generate the Grafana dashboard JSON and the alert rules (dashboards and alerts as code).

    python scripts/build_dashboard.py      # writes infra/grafana/provisioning/{dashboards/json,alerting}/

Never hand-edit the generated files: change this script and re-run it (Grafana re-reads provisioning on restart).
"""

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1] / "infra" / "grafana" / "provisioning"
DS = {"type": "grafana-postgresql-datasource", "uid": "warehouse"}
RUNBOOKS = "https://github.com/H1406/lakehouse_for_financial_data/blob/main/runbooks"

LATEST_DAY = "(select max(trade_date) from marts.fct_rolling_volatility)"


def target(sql: str, fmt: str = "time_series", ref: str = "A") -> dict:
    return {"refId": ref, "datasource": DS, "rawSql": " ".join(sql.split()), "format": fmt, "rawQuery": True,
            "editorMode": "code"}


def panel(pid: int, title: str, kind: str, sql: str, x: int, y: int, w: int = 12, h: int = 8, fmt="time_series",
          unit: str | None = None, time_from: str | None = None, extra: dict | None = None) -> dict:
    p = {"id": pid, "title": title, "type": kind, "datasource": DS, "gridPos": {"x": x, "y": y, "w": w, "h": h},
         "targets": [target(sql, fmt)], "fieldConfig": {"defaults": {}, "overrides": []}, "options": {}}
    if unit:
        p["fieldConfig"]["defaults"]["unit"] = unit
    if time_from:
        p["timeFrom"] = time_from
    for k, v in (extra or {}).items():
        p[k] = v
    return p


def dashboard() -> dict:
    vol = ("select trade_date::timestamp as time, ticker as metric, {col} from marts.fct_rolling_volatility "
           "where $__timeFilter(trade_date::timestamp) and {col} is not null order by 1")
    panels = [
        panel(1, "20-day annualized volatility", "timeseries", vol.format(col="vol_20d"), 0, 0, unit="percentunit"),
        panel(2, "60-day annualized volatility", "timeseries", vol.format(col="vol_60d"), 12, 0, unit="percentunit"),
        panel(3, "Volatility regime: weekly average 20d vol (ticker x week)", "status-history",
              "select date_trunc('week', trade_date)::timestamp as time, ticker as metric, avg(vol_20d) as vol "
              "from marts.fct_rolling_volatility where $__timeFilter(trade_date::timestamp) and vol_20d is not null "
              "group by 1, 2 order by 1", 0, 8, w=24, h=9, unit="percentunit",
              extra={"fieldConfig": {"defaults": {"unit": "percentunit", "color": {"mode": "continuous-GrYlRd"}},
                                     "overrides": []}}),
        panel(4, "Drawdown from running high", "timeseries",
              "select trade_date::timestamp as time, ticker as metric, drawdown from marts.fct_drawdown "
              "where $__timeFilter(trade_date::timestamp) order by 1", 0, 17, unit="percentunit"),
        panel(5, "Latest 20d vol z-score (alert > 3)", "bargauge",
              f"select ticker, round(vol_20d_zscore::numeric, 2) as z from marts.fct_rolling_volatility "
              f"where trade_date = {LATEST_DAY} and vol_20d_zscore is not null order by z desc", 12, 17, fmt="table",
              extra={"options": {"orientation": "horizontal", "displayMode": "basic", "reduceOptions":
                                 {"values": True, "calcs": [], "fields": "/^z$/"}}}),
        panel(6, "60-day return correlation (latest day)", "table",
              "select ticker_a, ticker_b, round(corr_60d::numeric, 2) as corr_60d from marts.fct_correlation_60d "
              "where trade_date = (select max(trade_date) from marts.fct_correlation_60d) and corr_60d is not null "
              "order by corr_60d desc", 0, 25, fmt="table"),
        panel(7, "Intraday 60-minute volatility (crypto 24/7, equities delayed >= 10 min)", "timeseries",
              "select minute as time, ticker as metric, vol_60m from marts.fct_intraday_volatility "
              "where $__timeFilter(minute) and vol_60m is not null order by 1", 12, 25, unit="percentunit",
              time_from="2d"),
        panel(8, "Pipeline freshness: hours since last successful load", "table",
              "select coalesce(source, dag_id) as source, entity, max(finished_at) as last_success, "
              "round((extract(epoch from now() - max(finished_at)) / 3600)::numeric, 1) as hours_ago "
              "from ops.pipeline_run where status = 'success' group by 1, 2 order by hours_ago desc", 0, 33,
              fmt="table"),
        panel(9, "Reconciliation (last 50 checks)", "table",
              "select checked_at, source, entity, layer_from || ' -> ' || layer_to as hop, count_from, count_to, "
              "missing, duplicates, status from ops.reconciliation order by checked_at desc limit 50", 12, 33,
              fmt="table"),
        panel(10, "Stream events per minute", "timeseries",
              "select date_trunc('minute', written_at) as time, query_name as metric, sum(events) as events "
              "from ops.stream_metrics where $__timeFilter(written_at) group by 1, 2 order by 1", 0, 41,
              time_from="6h"),
        panel(11, "Stream max event lag (s)", "timeseries",
              "select date_trunc('minute', written_at) as time, query_name as metric, "
              "max(max_event_lag_seconds) as lag_s from ops.stream_metrics where $__timeFilter(written_at) "
              "group by 1, 2 order by 1", 12, 41, unit="s", time_from="6h"),
        panel(12, "Latest dbt results (non-pass first)", "table",
              "select unique_id, resource_type, status, failures, execution_time_s, generated_at from ops.dbt_result "
              "where invocation_id = (select invocation_id from ops.dbt_result order by generated_at desc limit 1) "
              "order by (status in ('pass', 'success')), unique_id", 0, 49, w=24, fmt="table"),
    ]
    return {"uid": "lakehouse", "title": "Lakehouse: volatility & pipeline health", "schemaVersion": 41,
            "time": {"from": "now-1y", "to": "now"}, "refresh": "1m", "tags": ["lakehouse"], "panels": panels}


def rule(uid: str, title: str, sql: str, op: str, threshold: float, severity: str, runbook: str, summary: str,
         dag_id: str | None = None, no_data: str = "OK") -> dict:
    labels = {"severity": severity}
    if dag_id:
        labels["dag_id"] = dag_id
    return {
        "uid": uid, "title": title, "condition": "C", "for": "0m", "noDataState": no_data, "execErrState": "Error",
        "labels": labels,
        "annotations": {"summary": summary, "runbook_url": f"{RUNBOOKS}/{runbook}.md"},
        "data": [
            {"refId": "A", "relativeTimeRange": {"from": 600, "to": 0}, "datasourceUid": "warehouse",
             "model": target(sql, "table")},
            {"refId": "C", "datasourceUid": "__expr__",
             "model": {"refId": "C", "type": "threshold", "expression": "A",
                       "conditions": [{"evaluator": {"type": op, "params": [threshold]}}]}},
        ],
    }


def alert_rules() -> dict:
    rules = [
        rule("vol_spike", "Volatility spike",
             f"select ticker, vol_20d_zscore as z from marts.fct_rolling_volatility "
             f"where trade_date = {LATEST_DAY} and vol_20d_zscore is not null",
             "gt", 3, "info", "dbt_failure", "20d vol z-score > 3 for {{ $labels.ticker }} (business alert)"),
        rule("missed_daily_load", "Missed daily load",
             # Weekend-aware: Fri 23:00 -> Mon 23:00 is a normal 72 h gap, so Sat, Sun and Mon before 23:30 count 0.
             "select case when extract(isodow from now()) in (6, 7) "
             "or (extract(isodow from now()) = 1 and now()::time < '23:30') then 0 else "
             "coalesce(extract(epoch from now() - max(finished_at)) / 3600, 999) end as hours "
             "from ops.pipeline_run where dag_id = 'prices_eod_daily' and task_id = 'copy_silver_to_pg' "
             "and status = 'success'",
             "gt", 30, "high", "api_failure", "No successful prices_eod_daily load for > 30 h",
             dag_id="prices_eod_daily"),
        rule("stream_stalled", "Stream stalled",
             "select coalesce(extract(epoch from now() - max(written_at)) / 60, 9999) as minutes "
             "from ops.stream_metrics where query_name = 'bronze_trades' and events > 0",  # heartbeats have 0
             "gt", 5, "high", "stream_stalled", "No crypto trades for > 5 minutes"),
        rule("reconciliation_mismatch", "Reconciliation mismatch",
             "select count(*) as mismatches from ops.reconciliation "
             "where status = 'mismatch' and checked_at > now() - interval '24 hours'",
             "gt", 0, "high", "reconciliation_mismatch", "ops.reconciliation has mismatches in the last 24 h"),
        rule("dbt_failure", "dbt failure",
             "select count(*) as failed from ops.dbt_result where status in ('error', 'fail') and invocation_id = "
             "(select invocation_id from ops.dbt_result order by generated_at desc limit 1)",
             "gt", 0, "high", "dbt_failure", "Latest dbt build has failed models or tests", dag_id="dbt_build"),
        rule("quarantine_spike", "Quarantine spike",
             "select coalesce(max(rows_quarantined::float / nullif(rows_read, 0)), 0) as rate from ops.pipeline_run "
             "where started_at > now() - interval '1 hour' and rows_read > 0",
             "gt", 0.05, "medium", "api_failure", "A run quarantined > 5 % of its records (source schema drift?)"),
    ]
    return {"apiVersion": 1, "groups": [{"orgId": 1, "name": "lakehouse", "folder": "Lakehouse", "interval": "1m",
                                         "rules": rules}]}


if __name__ == "__main__":
    (ROOT / "dashboards" / "json" / "lakehouse.json").write_text(json.dumps(dashboard(), indent=1))
    (ROOT / "alerting" / "rules.json").write_text(json.dumps(alert_rules(), indent=1))
    print("wrote dashboard (12 panels) and 6 alert rules")
