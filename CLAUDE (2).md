# CLAUDE.md

Guidance for Claude Code in this repository.

## What this project is

A laptop-scale **lakehouse** for market volatility monitoring and financial AI agents. It has three sources:
- **Batch:** Tiingo EOD prices (CSV) and SEC EDGAR companyfacts (JSON), run by Airflow
- **Streaming:** Binance trades → Redpanda (Kafka API) → Spark Structured Streaming

The flow:
1. Both land in **Apache Iceberg** tables on **SeaweedFS** (S3), through a shared **Iceberg REST catalog**.
2. **PySpark** builds Bronze → Silver.
3. Silver is copied into **PostgreSQL 16**, where **dbt** builds the Gold marts.
4. The marts feed a **Grafana** dashboard with alerts, fact sheets in **pgvector** for a QA agent, and an **incident-triage agent**.

- Full spec, data model and schedule: **`docs/PLAN.md`** (read the relevant section before starting a task).
- Current state: **`docs/PROGRESS.md`** (read it at the start of every session; update it at the end).
- Why things are the way they are: **`docs/DECISIONS.md`**.
- Pinned versions: **`docs/VERSIONS.md`**.

## The owner is learning: work accordingly

This is a portfolio project for a Data Engineer trainee application. The owner must be able to **explain every design choice in an interview**.
- Before a non-trivial change, state the plan in 3–5 lines and the main trade-off.
- After making a design choice (partitioning, merge keys, watermark length, incremental strategy, tuning setting), add 2–4 lines to `docs/DECISIONS.md`: the decision, the alternatives, and why.
- Prefer clear, idiomatic code over clever code. No hidden magic or frameworks the plan doesn't name.
- When something is a laptop simplification of a production pattern, say so in a comment and in DECISIONS.md.

## Commands

```bash
make up            # full stack (SeaweedFS, iceberg-rest, Redpanda, Postgres+pgvector, Airflow, Grafana)
make up-core       # without Grafana and the stream services (lighter, for dbt/Spark work)
make down
make init          # buckets, Iceberg namespaces, Postgres databases/schemas/roles, ops tables
make backfill      # 5-year price backfill + EDGAR facts (monthly chunks)
make stream-start / make stream-stop
make dbt-build     # dbt source freshness + dbt build
make test          # ruff + pytest tests/unit (no network, no Docker)
make test-int      # integration tests; needs `make up-core`
make lint
make fault FAULT=api_500|schema_change|duplicate_batch|stream_stop
make triage INCIDENT=<id>
make eval          # QA retrieval metrics + triage fault results
```

dbt runs from `dbt/` with `--profiles-dir .`. Use `--target ci` for the seed-sized CI database.

## Repository map

- `infra/`: docker-compose, Grafana provisioning, Postgres init SQL (schemas, roles, grants)
- `docker/`: images. The Airflow image includes JDK 17, pyspark and **baked** Iceberg/Kafka JARs.
- `src/lakehouse/`: shared Python package (`config`, `ops`, `ingestion`, `iceberg`, `docs_gen`)
- `spark_jobs/`: PySpark entry points (streaming, Bronze → Silver, copy to Postgres, maintenance)
- `streaming/producer.py`: WebSocket → Redpanda
- `dags/`: Airflow 3 DAGs (thin: call functions in `src/` or `spark-submit` jobs in `spark_jobs/`)
- `dbt/`: staging → intermediate → marts, seeds, singular tests
- `agents/`: `qa/`, `triage/`, `eval/`
- `runbooks/`: Markdown runbooks (also the triage agent's knowledge base)
- `scripts/`: init, fault injection, perf data scaling, dashboard generation
- `tests/`: `unit/`, `unit/spark/`, `integration/`, `fixtures/` (recorded API responses)

## Non-negotiable rules

1. **Idempotent and re-runnable.** Running any batch task twice for the same logical date must give the same result.
   - Bronze: dynamic partition overwrite on the run's partition.
   - Silver: `MERGE INTO` on the documented unique key.
   - Postgres: upsert (`INSERT … ON CONFLICT`).
   - Never use a plain append on batch paths.
2. **Every pipeline task is instrumented.** Wrap it with `@instrumented` from `lakehouse.ops`, which writes `ops.pipeline_run`. Telemetry is **fail-open**: an `ops` write error must never fail the ETL. Every layer hop writes an `ops.reconciliation` row (count_from, count_to, missing, duplicates, late).
3. **Validate at the edge.** Every external record passes a pydantic contract in `ingestion/contracts.py`. Invalid records go to `bronze.quarantine` with a reason; they are never silently dropped. Fail the run if the quarantine rate is above 5 %.
4. **Secrets only from env.** Use `.env` (git-ignored) and `lakehouse.config`. Never hard-code tokens, never print them, never commit `.env`. Error messages stored in `ops` go through the scrubber.
5. **Don't guess API shapes.** Before writing a parser, record a real response into `tests/fixtures/` and build against that. Never invent fields. Unit tests never hit the network.
6. **Respect source limits.**
   - SEC EDGAR: descriptive `User-Agent` (from env), ≤ 10 requests/second.
   - Tiingo: stay under the account's hourly and daily caps.
   - Binance: reconnect with backoff.
7. **No look-ahead.** Fundamentals join to prices only where `filed_date <= trade_date`. A dbt test enforces this; don't weaken it.
8. **Agents are read-only.**
   - The triage agent may read `ops`, dbt results, Airflow logs (via `/api/v2`) and runbooks, and may write only its note and `ops.incident.agent_diagnosis`. It suggests; it never executes fixes.
   - The NL-SQL tool uses role `agent_ro`, `SELECT`-only, a table allowlist, a row limit and a statement timeout.
   - Scrub logs before sending them to an LLM.
9. **PII stays in `pii`.** Analytics uses `subscriber_key` (HMAC) only. Never add PII columns to `lake_silver`, `marts`, `ops` or `agent`.
10. **Verify versions; don't assume them.**
    - Pin exact image tags and library versions in `docs/VERSIONS.md`.
    - The Iceberg Spark runtime artifact must match the Spark minor version and Scala 2.13 (e.g. `iceberg-spark-runtime-4.0_2.13`).
    - The Kafka connector version must equal the Spark version.
    - Check Maven Central or PyPI before changing a pin.

## Conventions

- **Python:** 3.12, type hints, `ruff` formatting and lint, `pydantic` v2, `pydantic-settings` for config, `structlog` or stdlib logging with JSON output. Use small pure functions for transforms so they can be unit tested.
- **Airflow 3:** import from `airflow.sdk` (`dag`, `task`, `Asset`). Keep DAG files thin. Use dynamic task mapping per ticker. Default 3 retries with exponential backoff. Every DAG has `on_failure_callback` → notification + `ops.incident`. Downstream dbt runs are scheduled by **Assets**, not by sensors.
- **Spark:**
  - Build the session in `spark_jobs/common.py`: catalog `lake` = REST catalog; `s3.endpoint` = SeaweedFS; `s3.path-style-access=true`; AQE on.
  - No Python UDFs where a built-in function exists.
  - Always filter to the run's partition instead of scanning whole tables.
- **Iceberg:**
  - Tables are created in `src/lakehouse/iceberg/tables.py` (one place for schemas and partition specs).
  - Bronze/Silver metadata columns: `_run_id`, `_source`, `_ingested_at`, `_payload_hash`; Silver adds `_updated_at`.
  - Maintenance (compaction, snapshot expiry, orphan cleanup) runs only from the maintenance DAG.
- **Streaming:**
  - Explicit schema; event-time watermark of 2 minutes; checkpoints under `s3://lake/checkpoints/<query>`.
  - The streaming job runs as its own container, **not** as an Airflow task.
  - Changing a checkpointed query's logic may need a new checkpoint path: note it in DECISIONS.md.
- **dbt:**
  - Layer prefixes `stg_`, `int_`, `fct_`, `dim_`, `mart_`.
  - Every model has a description and key-column tests in its YAML. Facts get `dbt_utils.unique_combination_of_columns`.
  - Incremental models use the `merge` strategy with a documented lookback window.
  - Metric definitions live in `docs/METRICS.md`; keep them in sync.
- **SQL style:** lowercase keywords, CTEs over nested subqueries, explicit column lists (never `select *` in marts or in `INSERT … SELECT`).
- **Grafana:** dashboards are generated by `scripts/build_dashboard.py` and provisioned; don't hand-edit the JSON.
- **Commits:** small, one concern each, imperative subject (e.g. `Add Tiingo extractor with retries`). Run `make test` before each commit.

## How to take on a task

1. Read `docs/PROGRESS.md`, then the matching section of `docs/PLAN.md`.
2. If anything in the plan is ambiguous or conflicts with reality (an API differs, a version doesn't work), **stop and say so** with options. Don't silently change the design.
3. Write or extend the test (or fixture) first where practical, then the code.
4. Run it for real: unit tests, plus the relevant `make` target against the running stack. Show the evidence: row counts, `ops.reconciliation` rows, a dbt result, a screenshot path.
5. Update `docs/PROGRESS.md` (tick the boxes, note anything left open) and `docs/DECISIONS.md` if you made a choice.
6. Never mark a checklist item done if its acceptance criterion in PLAN.md section 13 hasn't been demonstrated.

## Known gotchas

- **REST catalog persistence:** `apache/iceberg-rest-fixture` defaults to a temporary SQLite database. Set `CATALOG_URI=jdbc:sqlite:file:/catalog/iceberg.db` with a mounted volume. Test with `docker compose down && up`.
- **SeaweedFS + Iceberg:** needs path-style access, an explicit endpoint and a region value (any, e.g. `us-east-1`). Set them for both PyIceberg (`s3.*` properties) and Spark (`spark.sql.catalog.lake.s3.*`), and set the same values in the REST catalog container (`CATALOG_S3_*` / AWS env vars).
- **MinIO is not used:** its community edition is archived and ships no images. Don't reintroduce it.
- **Tiingo** returns split/dividend-adjusted columns (`adjClose` etc.); use the adjusted series for returns and keep the raw ones.
- **EDGAR** companyfacts can be several MB per company; store the payload once in Bronze and explode in Spark, not in Python. Concept names differ across companies: map them through `dbt/seeds/concept_aliases.csv`.
- **Airflow 3** standalone mode is fine for the laptop. Its REST API is `/api/v2` (the triage agent needs a token from env).
- **Small files** from streaming are expected; compaction belongs to the maintenance DAG, not to the stream job.
- **RAM:** if containers get OOM-killed, use `make up-core` and stop the stream; set Spark driver memory to 2 GB.

## Definition of done (for any feature)

- Code + tests pass locally (`make test`, and `make test-int` if it touches the stack)
- Re-run of the same logical date is a no-op on counts
- `ops.pipeline_run` and `ops.reconciliation` rows are written
- Failure path tested (at least one test or a manual fault) and covered by a runbook if it can page
- `PROGRESS.md` updated; `DECISIONS.md` updated if a choice was made
