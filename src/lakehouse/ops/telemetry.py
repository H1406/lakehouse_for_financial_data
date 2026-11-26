"""Pipeline telemetry in the Postgres `ops` schema.

Telemetry writes (pipeline_run, reconciliation, incident) are fail-open: an ops error is logged, never raised.
Watermarks are *not* telemetry: they drive what gets loaded, so their errors do raise.
"""

import functools
import json
import logging
import os
import uuid
from collections.abc import Callable
from datetime import UTC, date, datetime
from typing import Any

import psycopg
import requests

from lakehouse.config import settings
from lakehouse.ops.scrub import scrub

log = logging.getLogger(__name__)


def connect() -> psycopg.Connection:
    return psycopg.connect(settings().pg_dsn, connect_timeout=5, autocommit=True)


def _fail_open(fn: Callable[..., Any]) -> Callable[..., Any]:
    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return fn(*args, **kwargs)
        except Exception as exc:  # noqa: BLE001 - telemetry must never fail the ETL
            log.warning("ops telemetry write failed (%s): %s", fn.__name__, scrub(str(exc)))
            return None

    return wrapper


def airflow_context() -> dict[str, str | None]:
    """dag/task/run ids. Inside a task we read the Task SDK context; in spark-submit
    subprocesses (BashOperator) Airflow exports them as AIRFLOW_CTX_* env vars."""
    try:
        from airflow.sdk import get_current_context

        ctx = get_current_context()
        return {
            "dag_id": ctx["dag"].dag_id,
            "task_id": ctx["ti"].task_id,
            "airflow_run_id": ctx["run_id"],
        }
    except Exception:  # noqa: BLE001 - not inside an Airflow task
        return {
            "dag_id": os.getenv("AIRFLOW_CTX_DAG_ID"),
            "task_id": os.getenv("AIRFLOW_CTX_TASK_ID"),
            "airflow_run_id": os.getenv("AIRFLOW_CTX_DAG_RUN_ID"),
        }


@_fail_open
def _run_started(run_id: str, ctx: dict, source: str | None, entity: str | None, logical_date: Any) -> None:
    with connect() as conn:
        conn.execute(
            """insert into ops.pipeline_run (run_id, airflow_run_id, dag_id, task_id, source, entity,
                                             logical_date, started_at, status)
               values (%s, %s, %s, %s, %s, %s, %s, now(), 'running')""",
            (run_id, ctx["airflow_run_id"], ctx["dag_id"], ctx["task_id"], source, entity, logical_date),
        )


@_fail_open
def _run_finished(run_id: str, status: str, stats: dict, exc: BaseException | None) -> None:
    with connect() as conn:
        conn.execute(
            """update ops.pipeline_run set finished_at = now(), status = %s, rows_read = %s, rows_written = %s,
                      rows_quarantined = %s, error_class = %s, error_message_scrubbed = %s
               where run_id = %s""",
            (
                status,
                stats.get("rows_read"),
                stats.get("rows_written"),
                stats.get("rows_quarantined"),
                type(exc).__name__ if exc else None,
                scrub(str(exc)) if exc else None,
                run_id,
            ),
        )


def instrumented(source: str | None = None, entity: str | None = None) -> Callable:
    """Record one ops.pipeline_run row per call. The wrapped function may return a dict with
    rows_read / rows_written / rows_quarantined; it receives nothing extra."""

    def deco(fn: Callable) -> Callable:
        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            run_id = uuid.uuid4().hex
            logical_date = kwargs.get("logical_date")
            _run_started(run_id, airflow_context(), source or kwargs.get("source"),
                         entity or kwargs.get("entity"), logical_date)
            try:
                result = fn(*args, **kwargs)
            except BaseException as exc:
                # An exception may carry the counts it failed on (e.g. QuarantineRateExceeded), so alerts see them.
                _run_finished(run_id, "failed", getattr(exc, "stats", {}) or {}, exc)
                raise
            _run_finished(run_id, "success", result if isinstance(result, dict) else {}, None)
            return result

        return wrapper

    return deco


@_fail_open
def record_reconciliation(
    *,
    run_id: str,
    source: str,
    entity: str,
    logical_date: date | str | None,
    layer_from: str,
    layer_to: str,
    count_from: int,
    count_to: int,
    missing: int = 0,
    duplicates: int = 0,
    late: int = 0,
) -> str:
    status = "ok" if count_from == count_to and missing == 0 and duplicates == 0 else "mismatch"
    with connect() as conn:
        conn.execute(
            """insert into ops.reconciliation (run_id, source, entity, logical_date, layer_from, layer_to,
                   count_from, count_to, missing, duplicates, late, status)
               values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (run_id, source, entity, logical_date, layer_from, layer_to,
             count_from, count_to, missing, duplicates, late, status),
        )
    log.info("reconciliation %s %s %s->%s: %s (%s vs %s)", source, entity, layer_from, layer_to,
             status, count_from, count_to)
    return status


def get_watermark(source: str, entity: str) -> str | None:
    with connect() as conn:
        row = conn.execute(
            "select watermark_value from ops.watermark where source = %s and entity = %s", (source, entity)
        ).fetchone()
    return row[0] if row else None


def set_watermark(source: str, entity: str, value: str) -> None:
    with connect() as conn:
        conn.execute(
            """insert into ops.watermark (source, entity, watermark_value, updated_at) values (%s, %s, %s, now())
               on conflict (source, entity) do update set watermark_value = excluded.watermark_value,
                                                          updated_at = excluded.updated_at""",
            (source, entity, value),
        )


@_fail_open
def on_failure_callback(context: dict) -> None:
    """Airflow on_failure_callback: open an ops.incident row and notify the webhook if one is set."""
    ti = context.get("ti")
    dag_id = getattr(context.get("dag"), "dag_id", None)
    task_id = getattr(ti, "task_id", None)
    run_id = context.get("run_id")
    with connect() as conn:
        # One incident per failed DAG run: 12 mapped extract tasks failing together is one outage, not 12.
        # Atomic: the partial unique index incident_open_per_run makes concurrent callbacks (mapped tasks failing
        # in the same second) insert exactly one row; the others hit `on conflict do nothing`.
        row = conn.execute(
            """insert into ops.incident (alert_name, severity, dag_id, airflow_run_id, task_id)
               values (%s, 'high', %s, %s, %s)
               on conflict (dag_id, airflow_run_id) where resolved_at is null and airflow_run_id is not null
               do nothing
               returning incident_id""",
            (f"task_failed:{dag_id}.{task_id}", dag_id, run_id, task_id),
        ).fetchone()
    if row is None:
        return
    incident_id = row[0]
    url = settings().alert_webhook_url
    if url:
        text = f"[{datetime.now(UTC):%Y-%m-%d %H:%M}Z] incident {incident_id}: {dag_id}.{task_id} failed ({run_id})"
        requests.post(url, data=json.dumps({"text": text, "content": text}),
                      headers={"Content-Type": "application/json"}, timeout=5)
