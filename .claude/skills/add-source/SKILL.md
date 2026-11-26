---
name: add-source
description: >
  Step-by-step procedure for adding a new external data source (or a new entity from an existing source)
  to this lakehouse: fixture → contract → Bronze → Silver → Postgres → dbt → DAG → docs. Use when the user
  asks to add, onboard or ingest a new API, feed, dataset or table, e.g. "add FRED rates", "ingest
  dividends from X", "add a new source".
argument-hint: "<source name and what to ingest>"
---

# Add a new source

Follow these steps in order. The yfinance EOD prices path is the reference implementation. Mirror it rather
than inventing a new pattern. `CLAUDE.md` rules still apply, especially "stop and say so" if reality differs from the plan.

## 0. Plan first (tell the user before writing code)

In 3–5 lines, state:
- **Source:** endpoint, format, auth, rate limit, update cadence.
- **Batch or stream:** batch = Airflow DAG (steps 1–9). Stream = producer → Redpanda → `spark_jobs/stream_market.py`. For a stream, mirror `streaming/equity_producer.py` and stop to agree the design, since the steps below only partly apply.
- **Grain and unique key** of the Silver table (e.g. `(ticker, trade_date)`). This drives the MERGE, the upsert and the dbt tests.
- **Fetch window:** derived from the logical date (`prices_window`) or from a watermark (EDGAR's `ops.watermark`). It must be deterministic for reruns.
- **The main trade-off** of the design (partitioning, window size, where the payload is exploded).

## 1. Record a real response (never guess the shape)

- Add a function to `scripts/record_fixtures.py` and save the output to `tests/fixtures/<source>_<entity>.<json|jsonl>`.
- Trim large payloads but keep the structure. Include at least one awkward case (a split, an amendment, a null field).
- Secrets come from `.env`. Make sure the fixture contains no token or email.

## 2. Contract: `src/lakehouse/ingestion/contracts.py`

- Write a pydantic v2 model built from the fixture. Use `extra="forbid"` for flat rows so schema drift fails loudly. Use `extra="allow"` on large nested envelopes that Spark explodes later (see `CompanyFactsEnvelope`).
- Put range checks in `Field(gt=0, ...)` and cross-field checks in a `model_validator`.
- Add a test in `tests/unit/test_contracts.py`, using `fixture_or_skip`, that validates the recorded fixture.

## 3. Client and parser: `src/lakehouse/ingestion/<source>.py`

- Make HTTP calls through `lakehouse.ingestion.http` (retries plus a per-host `RateLimiter`). Respect the source's documented limit and add it to `CLAUDE.md` rule 6.
- Write `fetch_*` (network) and `parse_*` (pure: records → `(valid, bad)`) as separate functions. `bad` rows carry `{entity, reason, _payload}`.
- Unit-test `parse_*` against the fixture in `tests/unit/test_<source>.py`. No network.

## 4. Iceberg tables: `src/lakehouse/iceberg/tables.py`

- Add a `<COLS>` list and two `TABLES` entries:
  - `bronze.<entity>_raw`: columns + the raw payload column + `BATCH_META`, partitioned by `_logical_date`.
  - `silver.<entity>`: columns + `SILVER_META`. Leave it unpartitioned if it's small; otherwise justify the partitioning in DECISIONS.md.
- `make init` creates them, and `create_all()` is idempotent.

## 5. Ingestion tasks: `src/lakehouse/ingestion/tasks.py`

Mirror the three prices functions, each decorated with `@instrumented(source=..., entity=...)`:
- `extract_<entity>`: fetch → stage under `_raw_dir(source, logical_date)`.
- `land_<entity>_bronze`: parse → `write_quarantine(bad)` → `overwrite_slice(...)` (**never append**) → `_check_quarantine_rate` *after* writing.
- `reconcile_<entity>_source_bronze`: `slice_keys` + `record_reconciliation(layer_from="source", layer_to="bronze", ...)`.

## 6. Bronze → Silver: `spark_jobs/bronze_to_silver_<entity>.py`

- Copy `bronze_to_silver_prices.py`. Read only the run's slice (`_source` + `_logical_date`), dedupe with `latest_per_key(KEY)`, then `MERGE INTO` on KEY, updating only when `_payload_hash` changed.
- Record the bronze → silver reconciliation inside the job.
- Nested payloads are exploded here with built-in functions, not Python UDFs.
- Put `prepare()` in a unit test under `tests/unit/spark/`.

## 7. Silver → Postgres

- Add a `create table if not exists lake_silver.<entity>` with a primary key on KEY in `infra/postgres/init/sql/warehouse.sql`.
- Add `"<entity>": [KEY...]` to `KEYS` in `src/lakehouse/warehouse_copy.py`, and extend `tests/unit/test_warehouse_copy.py`.

## 8. DAG: `dags/<entity>.py`

- Copy `dags/prices_eod.py`: a thin file that calls step 5 and `spark_submit(...)`, uses `DEFAULT_ARGS`, and maps tasks per ticker or entity when that fits.
- Declare a new `Asset` in `dags/common.py` and set it as the `outlets` of the copy task.
- Add the Asset to the `dbt_build` schedule in `dags/dbt_build.py`. Use Assets, not sensors.
- Ask the user before adding a backfill variant.

## 9. dbt

- In `dbt/models/staging/_sources.yml`, add the table with a description. Add `freshness` only if the data's age is a real health signal, and write a comment explaining the thresholds or why freshness is skipped.
- Write `stg_<entity>.sql` with explicit columns, lowercase SQL and CTEs.
- In `_staging.yml`, add a description, `not_null` on KEY columns, and `dbt_utils.unique_combination_of_columns` on KEY.
- For any join to prices, enforce `filed_date <= trade_date`-style no-look-ahead, and add or extend a singular test.
- New metrics go in `docs/METRICS.md`.

## 10. Failure path and docs

- Use a runbook in `runbooks/` if this source can page: reuse `api_failure.md` and add a section, or write a new one.
- Test at least one failure, either a unit test of quarantine or rate-exceeded behaviour, or a manual fault.
- In `docs/DECISIONS.md`, write 2–4 lines on each choice from step 0 (key, window, partitioning, freshness), with the alternatives.
- Update `docs/PLAN.md` §4 (the source table), `docs/VERSIONS.md` for any new library (check PyPI first), `docs/COMPONENTS.md`, and `docs/PROGRESS.md`.

## Done when (show the evidence)

- `make test` passes, and `make test-int` too if you touched the stack.
- Run the DAG twice for the same logical date. The Bronze, Silver and `lake_silver` counts must not change on the second run.
- `ops.pipeline_run` rows exist, and the source→bronze and bronze→silver `ops.reconciliation` rows show `ok`.
- `make dbt-build` is green with the new tests.

Report the row counts and reconciliation rows to the user. Don't just say it works.
