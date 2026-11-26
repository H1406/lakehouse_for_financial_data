"""Guarded natural-language-SQL tool: the model writes SQL, this module decides whether it may run.

Defence in depth, each layer independent:
  1. parse with sqlglot: exactly one statement, and it must be a query (SELECT / UNION / WITH ... SELECT)
  2. no data-modifying or DDL node anywhere in the tree (e.g. a CTE containing INSERT)
  3. every table is in the marts allowlist (CTE names excepted); no other schema
  4. no dangerous functions (pg_*, dblink, lo_*, set_config, ...)
  5. wrapped in an outer LIMIT
  6. executed as role agent_ro (SELECT on marts only), in a read-only transaction, with statement_timeout
"""

import sqlglot
from sqlglot import exp

from agents.core.db import connect

ALLOWED_TABLES = {
    "dim_company", "fct_rolling_volatility", "fct_drawdown", "fct_correlation_60d", "fct_fundamentals_asof",
    "fct_intraday_volatility", "mart_fact_sheet_inputs",
}
MAX_ROWS = 100
FORBIDDEN_NODES = (exp.Insert, exp.Update, exp.Delete, exp.Merge, exp.Create, exp.Drop, exp.Alter, exp.Command,
                   exp.Into, exp.TruncateTable, exp.Grant)
FORBIDDEN_FUNC_PREFIXES = ("pg_", "dblink", "lo_", "set_config", "current_setting", "query_to_xml", "copy")


class SQLRejected(ValueError):
    pass


def check_sql(sql: str) -> str:
    """Return the SQL to execute (wrapped in a LIMIT), or raise SQLRejected with the reason."""
    try:
        statements = [s for s in sqlglot.parse(sql, read="postgres") if s is not None]
    except sqlglot.errors.ParseError as exc:
        raise SQLRejected(f"could not parse SQL: {str(exc)[:200]}") from exc
    if len(statements) != 1:
        raise SQLRejected("exactly one statement is allowed")
    stmt = statements[0]
    if not isinstance(stmt, exp.Query):
        raise SQLRejected("only SELECT queries are allowed")
    for node in stmt.walk():
        if isinstance(node, FORBIDDEN_NODES):
            raise SQLRejected(f"{type(node).__name__} is not allowed")
        if isinstance(node, exp.Func):
            name = (node.name if isinstance(node, exp.Anonymous) else node.sql_name()).lower()
            if name.startswith(FORBIDDEN_FUNC_PREFIXES):
                raise SQLRejected(f"function {name} is not allowed")
    ctes = {c.alias_or_name.lower() for c in stmt.find_all(exp.CTE)}
    for table in stmt.find_all(exp.Table):
        name, schema = table.name.lower(), (table.db or "").lower()
        if not schema and name in ctes:
            continue
        if schema not in ("", "marts") or name not in ALLOWED_TABLES:
            raise SQLRejected(f"table {table.sql()} is not allowed; use marts tables: {sorted(ALLOWED_TABLES)}")
    return f"select * from ({stmt.sql(dialect='postgres')}) as guarded_query limit {MAX_ROWS}"


def run_sql(sql: str) -> dict:
    safe = check_sql(sql)
    with connect("agent_ro", timeout_ms=5000) as conn:
        conn.execute("set search_path to marts")
        cur = conn.execute(safe)
        rows = cur.fetchall()
    cols = [d.name for d in cur.description]
    return {"columns": cols, "rows": [[r[c] for c in cols] for r in rows], "row_count": len(rows),
            "limited_to": MAX_ROWS}


def describe_tables() -> dict:
    with connect("agent_ro") as conn:
        rows = conn.execute(
            "select table_name, column_name, data_type from information_schema.columns "
            "where table_schema = 'marts' and table_name = any(%s) order by table_name, ordinal_position",
            (sorted(ALLOWED_TABLES),)).fetchall()
    out: dict[str, list[str]] = {}
    for r in rows:
        out.setdefault(r["table_name"], []).append(f"{r['column_name']} {r['data_type']}")
    return out
