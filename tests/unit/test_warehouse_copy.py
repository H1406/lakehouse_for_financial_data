from lakehouse.warehouse_copy import upsert_sql


def test_upsert_sql_updates_only_non_key_columns():
    q = upsert_sql("prices_eod", ["ticker", "trade_date", "close", "_updated_at"]).as_string(None)
    assert 'on conflict ("ticker", "trade_date") do update' in q
    assert '"close" = excluded."close"' in q and '"ticker" = excluded' not in q


def test_upsert_sql_uses_null_safe_constraint_for_facts():
    q = upsert_sql("financial_facts", ["cik", "concept", "unit", "period_start", "period_end", "accession_no",
                                       "value"]).as_string(None)
    assert "on conflict on constraint financial_facts_key" in q
