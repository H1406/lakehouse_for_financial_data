"""Audit log of agent runs in agent.run_log. Fail-open: logging problems never break an agent run."""

import json
import logging
import re
import uuid

import psycopg

from lakehouse.config import settings
from lakehouse.ops import scrub

log = logging.getLogger(__name__)

MAX_STRING = 4_000       # per string value
MAX_PAYLOAD = 60_000     # whole serialised payload; above this only a scrubbed text excerpt is kept
_SECRET_KEY = re.compile(r"(?i)(token|password|passwd|secret|api[_-]?key|authorization)")


def scrub_tree(value: object, key: str = "") -> object:
    """Scrub every string inside a JSON-like value but keep its structure.

    Scrubbing the serialised JSON instead would replace every quoted key and value with '***' (the scrubber
    masks quoted strings), which made logged tool results unreadable: no doc_id survived. Values under a
    secret-looking key are masked whole, which is what the key=value rule did on the flat text.
    """
    if key and _SECRET_KEY.search(key) and value not in (None, ""):
        return "***"
    if isinstance(value, str):
        return scrub(value, max_len=MAX_STRING)
    if isinstance(value, dict):
        return {str(k): scrub_tree(v, str(k)) for k, v in value.items()}
    if isinstance(value, list | tuple):
        return [scrub_tree(v) for v in value]
    if value is None or isinstance(value, bool | int | float):
        return value
    return scrub(str(value), max_len=MAX_STRING)  # dates, Decimals, ...


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
            text = json.dumps(scrub_tree(payload or {}))
            if len(text) > MAX_PAYLOAD:
                text = json.dumps({"truncated": True, "text": scrub(text, max_len=MAX_PAYLOAD)})
            with psycopg.connect(settings().dsn_for("agent_rw"), autocommit=True, connect_timeout=5) as conn:
                conn.execute(
                    "insert into agent.run_log (run_id, agent, step, event, tool_name, payload) "
                    "values (%s, %s, %s, %s, %s, %s::jsonb)",
                    (self.run_id, self.agent, step, event, tool_name, text),
                )
        except Exception as exc:  # noqa: BLE001
            log.warning("agent.run_log write failed: %s", type(exc).__name__)
