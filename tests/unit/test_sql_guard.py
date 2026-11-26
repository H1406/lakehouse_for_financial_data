import pytest

from agents.qa.sql_guard import SQLRejected, check_sql


@pytest.mark.parametrize("sql", [
    "select ticker, vol_20d from fct_rolling_volatility where ticker = 'NVDA'",
    "select * from marts.dim_company",
    "with v as (select ticker, max(vol_20d) m from fct_rolling_volatility group by 1) select * from v order by m desc",
    "select a.ticker from fct_drawdown a join dim_company c on c.ticker = a.ticker "
    "union select ticker from mart_fact_sheet_inputs",
])
def test_allowed_queries_are_wrapped_in_a_limit(sql):
    out = check_sql(sql)
    assert out.lower().startswith("select * from (") and out.endswith("limit 100")


@pytest.mark.parametrize("sql, reason", [
    ("delete from fct_drawdown", "only SELECT"),
    ("select 1; drop table marts.dim_company", "exactly one statement"),
    ("select * from ops.incident", "not allowed"),
    ("select * from pii.subscribers", "not allowed"),
    ("select * from lake_silver.prices_eod", "not allowed"),
    ("select * from pg_catalog.pg_authid", "not allowed"),
    ("select pg_sleep(10)", "pg_sleep"),
    ("select * from dim_company where pg_read_file('/etc/passwd') is not null", "pg_read_file"),
    ("with x as (insert into marts.dim_company values ('X') returning *) select * from x", "not allowed"),
    ("select * into marts.evil from dim_company", "not allowed"),
    ("update dim_company set name = 'x'", "only SELECT"),
    ("select set_config('statement_timeout', '0', false)", "set_config"),
])
def test_rejected_queries(sql, reason):
    with pytest.raises(SQLRejected, match=reason):
        check_sql(sql)
