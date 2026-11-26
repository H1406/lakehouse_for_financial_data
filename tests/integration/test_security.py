"""Least-privilege proofs against the live warehouse (make test-int)."""

import psycopg
import pytest

from lakehouse.config import settings


def conn(role: str) -> psycopg.Connection:
    dsn = settings().pg_dsn if role == "etl_writer" else settings().dsn_for(role)
    return psycopg.connect(dsn, autocommit=True)


@pytest.mark.parametrize("role", ["grafana_ro", "agent_ro", "agent_rw", "etl_writer"])
def test_only_pii_admin_can_read_pii(role):
    with conn(role) as c, pytest.raises(psycopg.errors.InsufficientPrivilege):
        c.execute("select email from pii.subscribers limit 1")


def test_pii_admin_can_read_pii():
    with conn("pii_admin") as c:
        assert c.execute("select count(*) from pii.subscribers").fetchone()[0] >= 0


def test_no_contact_columns_outside_pii():
    with conn("pii_admin") as c:  # information_schema shows only what the role can see; pii_admin sees marts
        rows = c.execute("""select table_schema, table_name, column_name from information_schema.columns
                            where table_schema in ('marts', 'ops', 'lake_silver', 'agent', 'staging')
                              and column_name ~* '(email|phone)'""").fetchall()
    assert rows == []


def test_triage_role_can_write_only_its_two_incident_columns():
    with conn("agent_rw") as c:
        c.execute("update ops.incident set agent_diagnosis = agent_diagnosis where false")  # allowed column
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            c.execute("update ops.incident set human_verdict = 'x' where false")
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            c.execute("delete from ops.incident where false")


def test_agent_ro_cannot_write_marts_or_read_lake_silver():
    with conn("agent_ro") as c:
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            c.execute("insert into marts.dim_company (ticker) values ('X')")
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            c.execute("select 1 from lake_silver.prices_eod limit 1")
