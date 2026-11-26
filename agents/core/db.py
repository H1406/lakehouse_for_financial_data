"""Least-privilege Postgres access for agents: every session is read-only and time-limited unless a write is
explicitly needed (triage writing its two ops.incident columns)."""

import psycopg
from psycopg.rows import dict_row

from lakehouse.config import settings


def connect(role: str, *, read_only: bool = True, timeout_ms: int = 5000) -> psycopg.Connection:
    opts = f"-c statement_timeout={timeout_ms}" + (" -c default_transaction_read_only=on" if read_only else "")
    return psycopg.connect(settings().dsn_for(role), autocommit=True, connect_timeout=5, options=opts,
                           row_factory=dict_row)


def query(role: str, sql: str, params: tuple | dict = (), *, limit: int = 200) -> list[dict]:
    with connect(role) as conn:
        return conn.execute(sql, params).fetchmany(limit)
