"""Agents HTTP service (own container, holds the embedding model in memory).

    POST /fact-sheets/refresh   render fact sheets + runbooks, re-embed changed docs   (called by the fact_sheets DAG)
    POST /grafana-webhook       Grafana alert -> ops.incident (optionally auto-triage)  (Grafana contact point)
    POST /triage   {"incident_id": 7}
    POST /ask      {"question": "..."}
    GET  /health

Laptop tool: no auth, so docker-compose publishes the port on 127.0.0.1 only.
"""

import json
import logging
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from agents.qa.agent import ask
from agents.retrieval.search import Embedder, refresh_index
from agents.triage.agent import triage
from lakehouse.ops import connect

logging.basicConfig(level=logging.INFO, format='{"ts":"%(asctime)s","level":"%(levelname)s","msg":"%(message)s"}')
log = logging.getLogger("agents")
EMBEDDER = Embedder()
AUTO_TRIAGE = os.getenv("TRIAGE_ON_ALERT", "false").lower() == "true"


def incidents_from_grafana(payload: dict) -> list[int]:
    """One ops.incident per firing alert, unless the same alert already has an open incident in the last hour."""
    ids = []
    with connect() as conn:  # etl_writer: the alert receiver records incidents, it is not the agent
        for a in payload.get("alerts", []):
            if a.get("status") != "firing":
                continue
            labels = a.get("labels", {})
            name = f"grafana:{labels.get('alertname', 'unknown')}"
            open_ = conn.execute(
                "select incident_id from ops.incident where alert_name = %s and resolved_at is null "
                "and detected_at > now() - interval '1 hour'", (name,)).fetchone()
            if open_:
                continue
            # Keep the alert's own words: WHICH stream / ticker / rule fired and why. Without them the triage agent
            # only sees the rule's name (it once blamed the closed equity market for a stopped crypto feed).
            details = {"source": "grafana", "labels": labels, "summary": a.get("annotations", {}).get("summary"),
                       "runbook_url": a.get("annotations", {}).get("runbook_url"), "values": a.get("values"),
                       "starts_at": a.get("startsAt")}
            ids.append(conn.execute(
                "insert into ops.incident (alert_name, severity, dag_id, details) values (%s, %s, %s, %s::jsonb) "
                "returning incident_id",
                (name, labels.get("severity", "high"), labels.get("dag_id"), json.dumps(details))).fetchone()[0])
    return ids


class Handler(BaseHTTPRequestHandler):
    def _send(self, status: int, body: dict) -> None:
        data = json.dumps(body, default=str).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:  # noqa: N802
        self._send(200, {"ok": True}) if self.path == "/health" else self._send(404, {"error": "not found"})

    def do_POST(self) -> None:  # noqa: N802
        try:
            body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))) or b"{}")
            if self.path == "/fact-sheets/refresh":
                self._send(200, refresh_index(EMBEDDER))
            elif self.path == "/grafana-webhook":
                ids = incidents_from_grafana(body)
                if AUTO_TRIAGE:
                    for i in ids:
                        threading.Thread(target=triage, args=(i,), kwargs={"embedder": EMBEDDER}, daemon=True).start()
                self._send(200, {"incidents": ids, "auto_triage": AUTO_TRIAGE})
            elif self.path == "/triage":
                self._send(200, triage(int(body["incident_id"]), embedder=EMBEDDER))
            elif self.path == "/ask":
                self._send(200, ask(str(body["question"]), embedder=EMBEDDER))
            else:
                self._send(404, {"error": "not found"})
        except Exception as exc:  # noqa: BLE001
            log.exception("request failed")
            self._send(500, {"error": f"{type(exc).__name__}: {str(exc)[:500]}"})


if __name__ == "__main__":
    port = int(os.getenv("AGENTS_PORT", "8090"))
    log.info("agents service on :%d (auto_triage=%s)", port, AUTO_TRIAGE)
    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()
