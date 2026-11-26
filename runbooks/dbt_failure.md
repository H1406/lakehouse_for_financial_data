# Runbook: dbt build / freshness failure

**Signals:** `dbt_source_freshness` or `dbt_build` failed; `ops.dbt_result` rows with status `error` / `fail`.

## Diagnose
1. `select unique_id, status, failures, message_scrubbed from ops.dbt_result where status in ('error','fail','warn') order by generated_at desc limit 20;`
2. Freshness error on `prices_eod` → the upstream `prices_eod_daily` DAG has not loaded (see api_failure.md). On a long holiday weekend, a warn is expected.
3. Test failures:
   - `assert_no_lookahead` → a join to fundamentals uses `filed_date > trade_date`. Never weaken the test; fix the model.
   - `assert_returns_sane` → a bad price or an unadjusted split; check `adj_close` for that ticker/date in `lake_silver.prices_eod`.
   - `unique_combination_of_columns` → duplicate keys upstream; check reconciliation for the same run.
4. Model errors (SQL) → `cd dbt && dbt build -s <model>` inside the airflow container and read `logs/dbt.log`.

## Fix
- Fix data upstream and rerun the source DAG (the asset triggers dbt again), or fix the model and run `make dbt-build`.

## Freshness error on crypto_bars_1m
`lake_silver.crypto_bars_1m` older than 120 min → the 15-minute copy DAG (`stream_silver_15m`) is not running (paused DAGs, Airflow down) or the stream itself stalled (see stream_stalled.md). Check `ops.pipeline_run` for `copy_bars_to_pg`.

## Airflow 3: tasks failing with `TypeError: fromisoformat: argument must be str`
A run created by `airflow dags trigger` or the UI has no logical date (`ds` is None). DAGs must use `common.run_date(dag_run)` / `common.RUN_DATE`, never `ds`.
