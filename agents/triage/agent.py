"""Incident-triage agent: reads the evidence for an ops.incident and writes a diagnosis note.

It SUGGESTS only. Every tool is a read (ops tables, dbt results, Airflow task logs via /api/v2, runbooks). Its only
writes are incidents/<id>.md and the two columns it is granted on ops.incident (agent_diagnosis,
triage_note_path). Logs are scrubbed before the model sees them.
"""

import argparse
import json
import re
import urllib.parse
from datetime import UTC, datetime
from pathlib import Path

import requests

from agents.core.db import connect, query
from agents.core.llm import DeepSeekClient
from agents.core.loop import Guardrails, run_agent
from agents.core.tools import Tool, params
from agents.core.trace import RunLog
from agents.retrieval.search import Embedder, hybrid_search
from lakehouse.config import settings
from lakehouse.ops import scrub

CATEGORIES = ["upstream_api_outage", "source_schema_drift", "duplicate_load", "stream_source_stalled",
              "stream_processing_failure", "dbt_model_or_test_failure", "data_freshness", "infrastructure",
              "unknown"]
NOTES_DIR = Path(__file__).resolve().parents[2] / "incidents"

SYSTEM = """You are the on-call data engineer's triage assistant for a lakehouse (Airflow batch DAGs, a Spark
streaming job fed by Redpanda, Iceberg Bronze/Silver, dbt marts in Postgres). Now: {now} UTC.

Investigate the incident with the tools, then write a triage note. The incident may be hours old: always pass
the incident's detected_at as `until` to the time-window tools, so you see the state AT the incident, not now.
You only SUGGEST; never claim you fixed
anything. Base every claim on tool output and quote it. Do not repeat identical calls.

How the pipeline reports problems:
- ops.pipeline_run: one row per task run (status, error_class, scrubbed error, rows_quarantined).
- ops.reconciliation: per layer hop counts; status 'mismatch' with duplicates > 0 means the same data was
  loaded twice; missing > 0 means rows were lost between layers.
- A run fails with QuarantineRateExceeded when > 5 % of source records fail the contract; "column mismatch"
  reasons in the error mean the source schema changed.
- ops.stream_metrics: one row per streaming micro-batch with data, plus 0-event "idle heartbeat" rows
  (batch_id -1) that Spark writes while it is alive but Kafka has no new data. So: no events but heartbeats
  continuing = the SOURCE stopped (producer / upstream feed), Spark is healthy; no rows at all = Spark is down.
- The equity feed is legitimately silent outside US market hours.

Write the final answer in exactly this Markdown format:
## Summary
<one or two sentences>
## Evidence
- <tool and the exact value/log line that supports the diagnosis>
## Likely cause
<cause>
## Confidence
<high | medium | low> - <why>
## Suggested next step
<concrete step, quoting the matching runbook section, e.g. runbook:api_failure>
CATEGORY: <one of: {categories}>"""


def _airflow_token() -> str:
    # Laptop: SIMPLE_AUTH_MANAGER_ALL_ADMINS issues an admin token without credentials. Production: a service
    # account with read-only (Viewer) permissions.
    r = requests.get(f"{settings().airflow_api_url}/auth/token", timeout=10)
    r.raise_for_status()
    return r.json()["access_token"]


def get_incident(incident_id: int) -> dict:
    rows = query("agent_ro", "select * from ops.incident where incident_id = %s", (incident_id,))
    return rows[0] if rows else {"error": f"incident {incident_id} not found"}


def _until(until: str | None) -> str:
    return until or datetime.now(UTC).isoformat()


def get_failed_runs(since_hours: int = 24, until: str | None = None) -> list[dict]:
    return query("agent_ro", """
        select dag_id, task_id, airflow_run_id, source, entity, logical_date, started_at, error_class,
               error_message_scrubbed, rows_read, rows_written, rows_quarantined
        from ops.pipeline_run where status = 'failed'
          and started_at between %(u)s::timestamptz - make_interval(hours => %(h)s) and %(u)s::timestamptz
        order by started_at desc""", {"u": _until(until), "h": int(since_hours)}, limit=25)


def get_recent_runs(dag_id: str, limit: int = 15) -> list[dict]:
    return query("agent_ro", """
        select task_id, airflow_run_id, logical_date, status, started_at, rows_read, rows_written,
               rows_quarantined, error_class
        from ops.pipeline_run where dag_id = %s order by started_at desc""", (dag_id,), limit=min(int(limit), 40))


def get_reconciliation_issues(since_hours: int = 24, until: str | None = None) -> list[dict]:
    return query("agent_ro", """
        select run_id, source, entity, logical_date, layer_from, layer_to, count_from, count_to, missing,
               duplicates, late, status, checked_at
        from ops.reconciliation where status = 'mismatch'
          and checked_at between %(u)s::timestamptz - make_interval(hours => %(h)s) and %(u)s::timestamptz
        order by checked_at desc""", {"u": _until(until), "h": int(since_hours)}, limit=25)


def get_stream_metrics(minutes: int = 30, until: str | None = None) -> list[dict]:
    """Per query: batches in the window ending at `until`, and minutes between the last batch and `until`."""
    return query("agent_ro", """
        with w as (select * from ops.stream_metrics where written_at <= %(u)s::timestamptz)
        select query_name, topic,
               count(*) filter (where written_at > %(u)s::timestamptz - make_interval(mins => %(m)s)) batches_in_window,
               coalesce(sum(events) filter (where written_at > %(u)s::timestamptz - make_interval(mins => %(m)s)), 0)
                   events_in_window,
               count(*) filter (where batch_id = -1
                                and written_at > %(u)s::timestamptz - make_interval(mins => %(m)s)) idle_heartbeats,
               max(written_at) filter (where events > 0) last_batch_with_events,
               max(written_at) filter (where batch_id = -1) last_idle_heartbeat,
               max(written_at) last_batch_before_until,
               round(extract(epoch from %(u)s::timestamptz - max(written_at) filter (where events > 0))::numeric
                     / 60, 1) minutes_without_events_at_until
        from w group by 1, 2""", {"u": _until(until), "m": int(minutes)})


def get_dbt_failures() -> list[dict]:
    return query("agent_ro", """
        select unique_id, resource_type, status, failures, message_scrubbed, generated_at
        from ops.dbt_result
        where invocation_id = (select invocation_id from ops.dbt_result order by generated_at desc limit 1)
          and status in ('error', 'fail', 'warn', 'runtime error')""")


def get_task_log(dag_id: str, run_id: str, task_id: str, try_number: int = 1, tail: int = 60,
                 map_index: int = -1) -> str:
    q = urllib.parse.quote
    url = (f"{settings().airflow_api_url}/api/v2/dags/{q(dag_id, safe='')}/dagRuns/{q(run_id, safe='')}"
           f"/taskInstances/{q(task_id, safe='')}/logs/{int(try_number)}?full_content=true"
           + (f"&map_index={int(map_index)}" if int(map_index) >= 0 else ""))
    r = requests.get(url, headers={"Authorization": f"Bearer {_airflow_token()}", "Accept": "application/json"},
                     timeout=20)
    if r.status_code != 200:
        return f"Airflow API returned HTTP {r.status_code}"
    lines = []
    for e in r.json().get("content", []):
        if isinstance(e, dict):
            ev = str(e.get("event", ""))
            if ev.startswith("::"):
                continue
            lines.append(f"{e.get('timestamp', '')} {e.get('level', '')} {ev}".strip())
            for err in e.get("error_detail") or []:  # Airflow 3 puts the exception here, not in `event`
                frames = [f"{f.get('filename', '').rsplit('/', 1)[-1]}:{f.get('lineno')} {f.get('name')}"
                          for f in (err.get("frames") or [])[-4:]]
                lines.append(f"  EXCEPTION {err.get('exc_type')}: {err.get('exc_value')}  at {' <- '.join(frames)}")
        else:
            lines.append(str(e))
    return "\n".join(scrub(line, max_len=600) for line in lines[-min(int(tail), 150):])


UNTIL = {"type": "string", "description": "ISO timestamp the window ends at; use the incident's detected_at "
         "(default: now)"}


def tools(embedder: Embedder) -> list[Tool]:
    def search_runbooks(query: str) -> list[dict]:
        return [{"doc_id": h["doc_id"], "section": h["section"], "content": h["content"]}
                for h in hybrid_search(query, embedder=embedder, k=4, kind="runbook")]

    return [
        Tool("get_incident", "The incident row (alert name, dag/task/run ids).",
             params(incident_id={"type": "integer"}, required=["incident_id"]), get_incident),
        Tool("get_failed_runs", "Failed task runs from ops.pipeline_run with scrubbed errors, in the window ending at "
             "`until`.", params(since_hours={"type": "integer", "description": "default 24"}, until=UNTIL),
             get_failed_runs),
        Tool("get_recent_runs", "Recent task runs (all statuses) of one DAG, incl. rows_quarantined.",
             params(dag_id={"type": "string"}, limit={"type": "integer"}, required=["dag_id"]), get_recent_runs),
        Tool("get_reconciliation_issues", "Reconciliation rows with status mismatch (missing/duplicates).",
             params(since_hours={"type": "integer", "description": "default 24"}, until=UNTIL),
             get_reconciliation_issues),
        Tool("get_stream_metrics", "Per streaming query: batches/events in the window ending at `until`, and how "
             "long no batch had arrived at `until`.",
             params(minutes={"type": "integer", "description": "window, default 30"}, until=UNTIL),
             get_stream_metrics),
        Tool("get_dbt_failures", "Failed/warned dbt nodes of the latest dbt invocation.", params(), get_dbt_failures),
        Tool("get_task_log", "Tail of an Airflow task log (scrubbed), via the Airflow REST API.",
             params(dag_id={"type": "string"}, run_id={"type": "string"}, task_id={"type": "string"},
                    try_number={"type": "integer", "description": "attempt, default 1"},
                    tail={"type": "integer", "description": "lines, default 60"},
                    map_index={"type": "integer", "description": "for mapped tasks (e.g. extract_prices per "
                               "ticker): 0, 1, ...; default -1 = not mapped"},
                    required=["dag_id", "run_id", "task_id"]), get_task_log),
        Tool("search_runbooks", "Hybrid search over the runbooks (diagnosis steps and fixes).",
             params(query={"type": "string"}, required=["query"]), search_runbooks),
    ]


def parse_category(note: str) -> str:
    m = re.search(r"CATEGORY:\s*([a-z_]+)", note)
    return m.group(1) if m and m.group(1) in CATEGORIES else "unknown"


def triage(incident_id: int, *, llm=None, embedder: Embedder | None = None, log: bool = True) -> dict:
    trace = RunLog("triage", enabled=log)
    inc = get_incident(incident_id)
    user = (f"Triage incident {incident_id}: {json.dumps(inc, default=str)}. Investigate and write the note.")
    result = run_agent(
        system=SYSTEM.format(now=datetime.now(UTC).strftime("%Y-%m-%d %H:%M"), categories=", ".join(CATEGORIES)),
        user=user, tools=tools(embedder or Embedder()), llm=llm or DeepSeekClient(),
        guard=Guardrails(max_steps=10, max_tool_calls=14, max_seconds=240), trace=trace)
    category = parse_category(result.answer)
    NOTES_DIR.mkdir(exist_ok=True)
    note_path = NOTES_DIR / f"{incident_id}.md"
    note_path.write_text(
        f"# Incident {incident_id}: {inc.get('alert_name', '?')}\n\n"
        f"_Triage agent, {datetime.now(UTC):%Y-%m-%d %H:%M} UTC, run `{trace.run_id}`, "
        f"stopped_by={result.stopped_by}, steps={result.steps}, tool calls={len(result.tool_calls)}, "
        f"{result.seconds}s. Suggestion only; a human decides._\n\n{result.answer}\n")
    diagnosis = f"[{category}] " + (re.search(r"## Likely cause\s*\n(.+?)(\n##|$)", result.answer, re.S) or
                                    re.search(r"(.+)", result.answer or "(empty)", re.S)).group(1).strip()[:1000]
    with connect("agent_rw", read_only=False) as conn:  # only these two columns are granted
        conn.execute("update ops.incident set agent_diagnosis = %s, triage_note_path = %s where incident_id = %s",
                     (diagnosis, f"incidents/{incident_id}.md", incident_id))
    return {"incident_id": incident_id, "category": category, "note_path": str(note_path),
            "stopped_by": result.stopped_by, "steps": result.steps, "seconds": result.seconds,
            "tool_calls": [c["name"] for c in result.tool_calls], "usage": result.usage, "run_id": trace.run_id}


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Run the triage agent on an ops.incident")
    p.add_argument("incident_id", type=int)
    out = triage(p.parse_args().incident_id)
    print(Path(out["note_path"]).read_text())
    print(json.dumps(out, indent=1, default=str))
