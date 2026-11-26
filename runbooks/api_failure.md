# Runbook: upstream API failure (Yahoo Finance / SEC EDGAR)

**Signals:** `extract_prices` / `extract_edgar` failed after retries. `ops.pipeline_run.error_class` is `YFRateLimitError` (Yahoo throttling), `YFPricesMissingError` / `YFTickerMissingError` (no data / symbol), `RetryableHTTPError` (EDGAR 429/5xx) or `HTTPError` (4xx).

## Diagnose
1. `select dag_id, task_id, error_class, error_message_scrubbed, started_at from ops.pipeline_run where status = 'failed' order by started_at desc limit 20;`
2. 5xx / timeouts on every ticker → vendor outage. A single ticker → that symbol (delisted, renamed).
3. EDGAR 403 → User-Agent problem (`SEC_USER_AGENT` in `.env`). `YFRateLimitError` → Yahoo is throttling this IP: wait 15–60 min. `YFTickerMissingError` on one ticker → renamed or delisted symbol: update `dbt/seeds/universe.csv`.
4. Delayed equity feed: `docker logs lakehouse-equity-producer-1` shows `fetch <T> failed` lines. One bad poll is skipped, and the next poll (or a restart, which re-reads 5 days) refills the gap.

## Fix
- Outage / 429: wait, then clear the failed task in Airflow. Reruns are idempotent (the window is derived from the logical date).
- Credentials: fix `.env`, `make up-core` to reload env, clear the task.
- Never "fix" by editing Bronze; the partition is rewritten on the rerun.

## Quarantine spike / QuarantineRateExceeded

**Signals:** `validate_and_land_bronze` failed with `QuarantineRateExceeded`; Grafana "Quarantine spike"; `ops.pipeline_run.rows_quarantined` high.

1. Read the reasons in `bronze.quarantine` for that `_source` / `_logical_date` (data browser: `/iceberg?table=bronze.quarantine`).
2. Reason `column mismatch: expected [...] got [...]` on **every** row → **source schema drift** (the vendor renamed/removed a column). Fix: update the contract in `ingestion/contracts.py` and the parser, record a new fixture, rerun the date. Bronze was not touched: the rate check runs before the overwrite.
3. Reason `Close: Input should be a valid number` only on the **latest date** → a provisional bar (Yahoo serves today's candle without a close for hours after the US close). Since 2026-11-26 these are skipped automatically; if it appears for older dates, it is real bad data.
4. Rerun the logical date after the fix; reruns are idempotent.
