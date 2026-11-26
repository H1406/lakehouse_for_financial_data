"""Inject one of four realistic faults so the triage agent can be evaluated on real incidents.

    make fault FAULT=api_500|schema_change|duplicate_batch|stream_stop      (host)
    make fault-cleanup FAULT=...                                             (undo it the way the runbook says)

Each fault runs through the production code path and creates a real ops.incident:
  api_500         prices DAG run with Yahoo's HTTP call failing with 500   -> task failure  -> on_failure_callback
  schema_change   prices DAG run with `Adj Close` renamed in the response  -> QuarantineRateExceeded -> callback
  duplicate_batch the same Bronze slice appended twice (bypassing overwrite) + Spark job -> reconciliation
                  mismatch -> Grafana alert -> webhook -> incident
  stream_stop     Binance producer stopped -> no micro-batches -> Grafana "Stream stalled" -> webhook -> incident
The incident id is printed and saved to data/faults/<fault>.json for agents/eval/triage_eval.py.
"""

import argparse
import json
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
COMPOSE = ["docker", "compose", "-f", str(REPO / "infra/docker-compose.yml"), "--env-file", str(REPO / ".env")]
FAULT_DATES = {"api_500": "2026-11-19", "schema_change": "2026-11-20", "duplicate_batch": "2026-11-24"}
EXPECTED = {"api_500": "upstream_api_outage", "schema_change": "source_schema_drift",
            "duplicate_batch": "duplicate_load", "stream_stop": "stream_source_stalled"}


# ---------------------------------------------------------------- host side

def sh(*cmd: str, check: bool = True) -> str:
    return subprocess.run(cmd, check=check, capture_output=True, text=True).stdout


def psql(sql: str) -> str:
    return sh(*COMPOSE, "exec", "-T", "postgres", "psql", "-U", "postgres", "-d", "warehouse", "-tAc", sql).strip()


def inside(*args: str) -> None:
    subprocess.run([*COMPOSE, "exec", "-T", "-e", "LAKEHOUSE_TASK_RETRIES=0", "airflow", "python",
                    "/opt/lakehouse/scripts/inject_fault.py", "--inside", *args], check=False)


def wait_for_incident(alert_name: str, since_id: int, timeout_s: int) -> int | None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        found = psql(f"select max(incident_id) from ops.incident where incident_id > {since_id} "
                     f"and alert_name like '{alert_name}%'")
        if found:
            return int(found)
        time.sleep(10)
    return None


def inject(fault: str) -> dict:
    before = int(psql("select coalesce(max(incident_id), 0) from ops.incident"))
    started = datetime.now(UTC)
    if fault in ("api_500", "schema_change"):
        inside("run_prices", fault, FAULT_DATES[fault])
        incident = wait_for_incident("task_failed:prices_eod_daily", before, 60)
    elif fault == "duplicate_batch":
        inside("duplicate", FAULT_DATES[fault])
        incident = wait_for_incident("grafana:Reconciliation mismatch", before, 240)
    elif fault == "stream_stop":
        sh(*COMPOSE, "--profile", "stream", "stop", "producer")
        incident = wait_for_incident("grafana:Stream stalled", before, 600)
    else:
        raise SystemExit(f"unknown fault {fault}")
    out = {"fault": fault, "incident_id": incident, "expected_category": EXPECTED[fault],
           "injected_at": started.isoformat(), "incident_after_s": round((datetime.now(UTC) - started).seconds)}
    path = REPO / "data" / "faults" / f"{fault}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(out, indent=1))
    return out


def cleanup(fault: str) -> None:
    if fault == "stream_stop":
        sh(*COMPOSE, "--profile", "stream", "start", "producer")
    elif fault == "duplicate_batch":
        inside("rerun_slice", FAULT_DATES[fault])  # runbook fix: rerun land (overwrite) + Spark for that date
    psql("update ops.incident set resolved_at = now() where resolved_at is null and incident_id in "
         "(select incident_id from ops.incident where detected_at > now() - interval '1 day')")


# ---------------------------------------------------------------- inside the airflow container

def run_prices_with_fault(fault: str, day: str) -> None:
    """Run prices_eod_daily in-process with the source patched (dag.test, no executor)."""
    from unittest import mock

    sys.path.insert(0, "/opt/lakehouse/dags")
    from airflow.models.dagbag import DagBag

    from lakehouse.ingestion import yahoo

    real = yahoo.fetch_daily
    if fault == "api_500":
        from curl_cffi.requests.exceptions import HTTPError

        def broken(ticker, start, end):
            raise HTTPError(f"HTTP Error 500: Internal Server Error for url: "
                            f"https://query2.finance.yahoo.com/v8/finance/chart/{ticker}")
    else:
        def broken(ticker, start, end):  # the source silently renamed a column
            return [{("AdjClose" if k == "Adj Close" else k): v for k, v in r.items()}
                    for r in real(ticker, start, end)]
    dag = DagBag(dag_folder="/opt/lakehouse/dags/prices_eod.py").get_dag("prices_eod_daily")
    with mock.patch.object(yahoo, "fetch_daily", broken):
        dag.test(logical_date=datetime.fromisoformat(day).replace(hour=23, tzinfo=UTC))


def duplicate_slice(day: str) -> None:
    import subprocess as sp

    from pyiceberg.expressions import And, EqualTo

    from lakehouse.iceberg.catalog import catalog

    t = catalog().load_table("bronze.prices_eod_raw")
    rows = t.scan(row_filter=And(EqualTo("_source", "yfinance"), EqualTo("_logical_date", day))).to_arrow()
    t.append(rows)  # the bug being simulated: a plain append instead of overwrite_slice
    print(f"appended {rows.num_rows} duplicate rows to the yfinance {day} slice")
    sp.run(["spark-submit", "--master", "local[*]", "--driver-memory", "2g",
            "/opt/lakehouse/spark_jobs/bronze_to_silver_prices.py", "--logical-date", day,
            "--run-id", f"fault_duplicate_batch_{day}", "--source", "yfinance"], check=True)


def rerun_slice(day: str) -> None:
    sys.path.insert(0, "/opt/lakehouse/dags")
    from airflow.models.dagbag import DagBag

    DagBag(dag_folder="/opt/lakehouse/dags/prices_eod.py").get_dag("prices_eod_daily").test(
        logical_date=datetime.fromisoformat(day).replace(hour=23, tzinfo=UTC))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("fault", nargs="?")
    p.add_argument("--cleanup", action="store_true")
    p.add_argument("--inside", nargs="+")
    a = p.parse_args()
    if a.inside:
        action, *rest = a.inside
        {"run_prices": run_prices_with_fault, "duplicate": duplicate_slice, "rerun_slice": rerun_slice}[action](*rest)
    elif a.cleanup:
        cleanup(a.fault)
    else:
        print(json.dumps(inject(a.fault), indent=1))
