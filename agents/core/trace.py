"""Audit log of agent runs in agent.run_log. Fail-open: logging problems never break an agent run."""

import json
import logging
import uuid

import psycopg

from lakehouse.config import settings
from lakehouse.ops import scrub

log = logging.getLogger(__name__)


class RunLog:
    def __init__(self, agent: str, run_id: str | None = None, enabled: bool = True):
        self.agent = agent
        self.run_id = run_id or uuid.uuid4().hex
        self.enabled = enabled

    def __call__(self, event: str, step: int | None = None, tool_name: str | None = None,
                 payload: dict | None = None) -> None:
        if not self.enabled:
            return
        try:
            text = scrub(json.dumps(payload or {}, default=str), max_len=20_000)
            try:
                json.loads(text)
            except ValueError:  # truncation/scrubbing broke the JSON: store it as a string
                text = json.dumps({"text": text})
            with psycopg.connect(settings().dsn_for("agent_rw"), autocommit=True, connect_timeout=5) as conn:
                conn.execute(
                    "insert into agent.run_log (run_id, agent, step, event, tool_name, payload) "
                    "values (%s, %s, %s, %s, %s, %s::jsonb)",
                    (self.run_id, self.agent, step, event, tool_name, text),
                )
        except Exception as exc:  # noqa: BLE001
            log.warning("agent.run_log write failed: %s", type(exc).__name__)
