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
