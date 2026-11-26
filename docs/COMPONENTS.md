# Component guide

A walk through every component of the lakehouse, with links to the code that implements it. Each section covers:
**what it does → where the code is → how it works → why it is built that way → how to check it.**

Line links (`file#L12`) point at the code as of the `stage-1-foundation-medallion` branch. Design rationale is
summarized here and argued in full in [DECISIONS.md](DECISIONS.md). Metric formulas are in [METRICS.md](METRICS.md),
pinned versions in [VERSIONS.md](VERSIONS.md), and the original spec in [PLAN.md](PLAN.md).

## Contents

1. [The whole pipeline in one picture](#1-the-whole-pipeline-in-one-picture)
2. [Following one record through the system](#2-following-one-record-through-the-system)
3. [Runtime and infrastructure](#3-runtime-and-infrastructure)
4. [Shared Python package `lakehouse`](#4-shared-python-package-lakehouse)
5. [Batch ingestion: source → Bronze](#5-batch-ingestion-source--bronze)
6. [Spark batch jobs: Bronze → Silver](#6-spark-batch-jobs-bronze--silver)
7. [Streaming: producers and the Spark stream app](#7-streaming-producers-and-the-spark-stream-app)
8. [Silver → Postgres copy](#8-silver--postgres-copy)
9. [Orchestration: Airflow DAGs](#9-orchestration-airflow-dags)
10. [Gold layer: dbt](#10-gold-layer-dbt)
11. [Data browser](#11-data-browser)
12. [Tests, fixtures and CI](#12-tests-fixtures-and-ci)
13. [Scripts and runbooks](#13-scripts-and-runbooks)
14. [Cross-cutting guarantees](#14-cross-cutting-guarantees)
15. [Measured results so far](#15-measured-results-so-far)
16. [Known limitations](#16-known-limitations)
17. [Agents: the shared loop, the QA agent and the triage agent](#17-agents-the-shared-loop-the-qa-agent-and-the-triage-agent)
18. [Grafana: dashboard and alerts as code](#18-grafana-dashboard-and-alerts-as-code)
19. [PII separation](#19-pii-separation)
20. [Fault injection and the performance harness](#20-fault-injection-and-the-performance-harness)

---

## 1. The whole pipeline in one picture

```
SOURCES                         INGESTION                         LAKEHOUSE (Iceberg on SeaweedFS S3, REST catalog)
───────                         ─────────                         ─────────────────────────────────────────────────
yfinance daily  ─┐  Airflow: extract → validate (pydantic) ─┬──> bronze.prices_eod_raw     ┐
SEC EDGAR JSON  ─┘           → quarantine → land Bronze     ├──> bronze.sec_companyfacts_raw├─ Spark MERGE ─> silver.prices_eod
                             → reconcile                    └──> bronze.quarantine          ┘                 silver.financial_facts
Binance WS ───> producer.py ────────┐
yfinance 1m ──> equity_producer.py ─┴─> Redpanda ──> Spark Structured Streaming (stream_market.py)
                (released ≥ 10 min late)     A: ──> bronze.crypto_trades ──(15-min Spark dedup)──> silver.crypto_trades
                                             B: ──> silver.crypto_bars_1m     (1-min windows, 2-min watermark)
                                             C: ──> bronze.equity_bars_1m ──(MERGE in-stream)──> silver.equity_bars_1m
                                                                       │
                              incremental upsert on _updated_at (PyIceberg → COPY → INSERT … ON CONFLICT)
                                                                       v
WAREHOUSE  PostgreSQL 16 + pgvector:  lake_silver.*  ──> dbt: staging → intermediate → marts (Gold)
ops schema: pipeline_run · watermark · reconciliation · stream_metrics · incident · dbt_result   ← written by every job
```

The split of responsibilities is the main design choice of the project:

| Layer | Engine | Why this engine |
|---|---|---|
| Source → Bronze (batch) | Python + PyIceberg inside Airflow tasks | small HTTP payloads; no JVM needed per extract |
| Source → Bronze (stream) | Spark Structured Streaming | checkpoints, event time, exactly-once offsets from Kafka |
| Bronze → Silver | Spark (`MERGE INTO`, `explode`) | file/table work on Iceberg: dedup, nested JSON, upserts |
| Silver → Postgres | PyIceberg + psycopg `COPY` | small volumes; avoids a JVM and a JDBC jar |
| Silver → Gold | dbt on Postgres | metric logic analysts can read, test and document in SQL |

---

## 2. Following one record through the system

These walkthroughs are the fastest way to understand how the components connect.

### 2.1 One daily price (AAPL, 2026-11-24)

1. **Schedule.** `prices_eod_daily` fires at 23:00 UTC on weekdays ([dags/prices_eod.py#L59](../dags/prices_eod.py#L59)).
   Its logical date `ds` is 2026-11-24.
2. **Extract.** A mapped task, one per ticker ([dags/prices_eod.py#L53](../dags/prices_eod.py#L53)), calls
   [`extract_prices`](../src/lakehouse/ingestion/tasks.py#L43). It fetches the deterministic window
   `[ds − 7 d, ds]` ([`prices_window`](../src/lakehouse/ingestion/tasks.py#L34)) with
   [`yahoo.fetch_daily`](../src/lakehouse/ingestion/yahoo.py#L42) and writes `data/raw/yfinance/2026-11-24/AAPL.json`.
3. **Validate and land.** [`land_prices_bronze`](../src/lakehouse/ingestion/tasks.py#L67) parses every file with
   [`parse_daily`](../src/lakehouse/ingestion/yahoo.py#L51). Each record must pass the pydantic contract
   [`YahooDailyRow`](../src/lakehouse/ingestion/contracts.py#L11), or it goes to quarantine with a reason. Valid rows
   replace the `(_source='yfinance', _logical_date=2026-11-24)` slice of `bronze.prices_eod_raw` in **one Iceberg
   commit** ([bronze_writer.py#L38-L41](../src/lakehouse/ingestion/bronze_writer.py#L38-L41)).
4. **Reconcile source → Bronze.** [`reconcile_prices_source_bronze`](../src/lakehouse/ingestion/tasks.py#L81) compares
   the parsed keys with the keys in the Bronze slice and writes one `ops.reconciliation` row.
5. **Bronze → Silver.** A BashOperator runs `spark-submit spark_jobs/bronze_to_silver_prices.py`. The job keeps the
   latest ingest per `(ticker, trade_date)` and MERGEs it into `silver.prices_eod`, updating only when the payload hash
   changed ([bronze_to_silver_prices.py#L28-L33](../spark_jobs/bronze_to_silver_prices.py#L28-L33)). Then it writes the
   Bronze → Silver reconciliation row.
6. **Silver → Postgres.** [`copy_table("prices_eod")`](../src/lakehouse/warehouse_copy.py#L49) reads Silver rows with
   `_updated_at` past the watermark and upserts them into `lake_silver.prices_eod`. The task then emits the Asset
   `silver_prices` ([dags/prices_eod.py#L49](../dags/prices_eod.py#L49)).
7. **Gold.** The Asset triggers `dbt_build` ([dags/dbt_build.py#L18](../dags/dbt_build.py#L18)). The row becomes a log
   return ([int_daily_returns.sql#L6](../dbt/models/intermediate/int_daily_returns.sql#L6)), then 20/60-day volatility
   and a z-score ([fct_rolling_volatility.sql](../dbt/models/marts/fct_rolling_volatility.sql)), then drawdown,
   correlation and point-in-time fundamentals.
8. **Telemetry.** Every step above wrote an `ops.pipeline_run` row through `@instrumented`.

### 2.2 One delayed equity bar (NVDA, 14:30 UTC)

1. [`equity_producer.py`](../streaming/equity_producer.py) polls `Ticker("NVDA").history(interval="1m")` every 60 s.
2. The bar for 14:30 is complete at 14:31. It is released only when `minute + 1 min + 10 min ≤ now`
   ([equity_producer.py#L59](../streaming/equity_producer.py#L59)), so it is published at about 14:41–14:42 to topic
   `equity_bars_1m` with key `NVDA`.
3. Spark query C ([stream_market.py#L252](../spark_jobs/stream_market.py#L252)) parses it with an explicit schema,
   appends it to `bronze.equity_bars_1m`, and MERGEs it into `silver.equity_bars_1m` on `(ticker, minute)`
   ([stream_market.py#L187-L191](../spark_jobs/stream_market.py#L187-L191)).
4. Every 15 minutes `stream_silver_15m` copies new Silver bars to Postgres.
   [`int_intraday_returns_1m`](../dbt/models/intermediate/int_intraday_returns_1m.sql) and
   [`fct_intraday_volatility`](../dbt/models/marts/fct_intraday_volatility.sql) turn them into 60-minute volatility.
   Measured end-to-end delay: **~661 s** from bar start to Kafka publish.

### 2.3 One Binance trade

WebSocket → [`producer.py`](../streaming/producer.py#L46) (key = symbol) → Redpanda → query A appends it to
`bronze.crypto_trades` ([stream_market.py#L130](../spark_jobs/stream_market.py#L130)) and query B folds it into a 1-minute
bar ([stream_market.py#L99-L100](../spark_jobs/stream_market.py#L99-L100)). Every 15 minutes
[`silver_crypto_trades.py`](../spark_jobs/silver_crypto_trades.py) dedups it into `silver.crypto_trades` by
`(symbol, trade_id)`.

### 2.4 One XBRL fact (Apple revenue, Q3 FY2026)

`sec_facts_weekly` → [`extract_edgar`](../src/lakehouse/ingestion/tasks.py#L96) checks the `submissions` endpoint and
skips the CIK if there's no 10-K/10-Q newer than its watermark ([tasks.py#L94-L95](../src/lakehouse/ingestion/tasks.py#L99-L100)).
Otherwise it stores the full companyfacts JSON (several MB) as **one Bronze row**. Spark
[`explode_facts`](../spark_jobs/bronze_to_silver_facts.py#L40) unnests taxonomy → concept → unit → fact into ~12k rows
per company, which are MERGEd with null-safe keys. dbt
[`int_fundamentals_quarterly`](../dbt/models/intermediate/int_fundamentals_quarterly.sql) then picks each filing's own
period and pivots the concepts into revenue, net income, and so on.

---

## 3. Runtime and infrastructure

### 3.1 Docker Compose stack

**Code:** [infra/docker-compose.yml](../infra/docker-compose.yml) · **Commands:** [Makefile](../Makefile)

| Service | Line | Image | Port (host) | Profile | Role |
|---|---|---|---|---|---|
| `seaweedfs` | [L32](../infra/docker-compose.yml#L32) | `chrislusf/seaweedfs:4.55` | 8333 | core | S3 object storage for all Iceberg files and checkpoints |
| `iceberg-rest` | [L44](../infra/docker-compose.yml#L44) | `lakehouse-iceberg-rest:local` | 8181 | core | Iceberg REST catalog, state in Postgres |
| `postgres` | [L62](../infra/docker-compose.yml#L62) | `pgvector/pgvector:0.8.7-pg16` | 5432 | core | warehouse, Airflow metadata, Iceberg catalog DB |
| `airflow` | [L82](../infra/docker-compose.yml#L82) | `lakehouse-airflow:local` | 8080 | core | `airflow standalone`, LocalExecutor; runs batch Spark and dbt |
| `data-browser` | [L117](../infra/docker-compose.yml#L117) | `lakehouse-airflow:local` | 127.0.0.1:8501 | core | read-only web GUI (section 11) |
| `redpanda` | [L161](../infra/docker-compose.yml#L161) | `redpandadata/redpanda:v26.3.1` | 9092 | stream | Kafka-API broker |
| `producer` | [L175](../infra/docker-compose.yml#L175) | `lakehouse-producer:local` | – | stream | Binance → `crypto_trades` |
| `equity-producer` | [L186](../infra/docker-compose.yml#L186) | `lakehouse-producer:local` | – | stream | delayed yfinance → `equity_bars_1m` |
| `agents` | [L126](../infra/docker-compose.yml#L126) | `lakehouse-agents:local` | 127.0.0.1:8090 | core | QA + triage agents, embeddings, fact-sheet indexing, Grafana alert receiver (§17) |
| `grafana` | [L148](../infra/docker-compose.yml#L148) | `grafana/grafana:13.2.7` | 127.0.0.1:3000 | core | dashboard + 6 alert rules (§18) |
| `spark-stream` | [L202](../infra/docker-compose.yml#L202) | `lakehouse-spark-stream:local` | – | stream | long-running Structured Streaming app |

Named volumes: `seaweed-data`, `pg-data`, `redpanda-data` ([L217-L219](../infra/docker-compose.yml#L217-L219)). Repo
folders (`src/`, `dags/`, `spark_jobs/`, `dbt/`, `scripts/`, `tests/`, `data/`) are bind-mounted into the containers,
so code edits apply without a rebuild.

YAML anchors `x-s3-env` and `x-app-env` (top of the file) give every service the same S3 endpoint, catalog URI and
Postgres host, so all engines see one consistent configuration.

**Why:** `make up-core` (no stream) fits dbt and Spark work into the 8 GB Docker VM. The stream is a separate
**service** with `restart: unless-stopped`, not an Airflow task: Airflow schedules batch work and only *monitors* the
stream ([§9.3](#93-stream_silver_15m)).

**Verify:** `make up && docker compose -f infra/docker-compose.yml --env-file .env ps`.

### 3.2 Container images

| Image | File | What it adds |
|---|---|---|
| Airflow | [docker/airflow.Dockerfile](../docker/airflow.Dockerfile) | JDK 17; Python deps installed against Airflow's constraints **minus the `certifi` pin** ([L14-L15](../docker/airflow.Dockerfile#L14-L15)); Iceberg JARs copied into pyspark's own `jars/` dir ([L21-L22](../docker/airflow.Dockerfile#L21-L22)); dbt in a separate venv ([L25-L26](../docker/airflow.Dockerfile#L25-L26)) |
| Spark stream | [docker/spark-stream.Dockerfile](../docker/spark-stream.Dockerfile) | Iceberg runtime + AWS bundle, Kafka connector 4.0.5 and its two deps, `hadoop-aws` + two small AWS SDK jars for `s3a://` checkpoints ([L8-L16](../docker/spark-stream.Dockerfile#L8-L16)) |
| Iceberg REST | [docker/iceberg-rest.Dockerfile](../docker/iceberg-rest.Dockerfile) | the upstream fixture + the Postgres JDBC driver on the classpath ([L4-L6](../docker/iceberg-rest.Dockerfile#L4-L6)) |
| Producer | [docker/producer.Dockerfile](../docker/producer.Dockerfile) | one slim image for both feeds (`websockets`, `confluent-kafka`, `yfinance`) |

**Why JARs are baked in:** `--packages` downloads at runtime, which is slow and breaks offline. Every version rule
(the Iceberg runtime matches the Spark minor version and Scala 2.13; the Kafka connector equals the Spark version) is
recorded in [VERSIONS.md](VERSIONS.md).

### 3.3 SeaweedFS (S3)

**Code:** [docker-compose.yml#L29](../infra/docker-compose.yml#L34), identities in
[infra/seaweedfs/s3.json](../infra/seaweedfs/s3.json). The bucket `lake` is created by `make init`
([Makefile](../Makefile), `weed shell` → `s3.bucket.create`).

Layout inside the bucket:

```
s3://lake/warehouse/<namespace>/<table>/data/*.parquet       Iceberg data files
s3://lake/warehouse/<namespace>/<table>/metadata/*           metadata.json, manifest lists, manifests (Avro)
s3://lake/checkpoints/v2/{bronze_trades,bars_1m,equity_bars}  Structured Streaming checkpoints (s3a)
```

**Gotchas handled:** `-ip.bind=0.0.0.0` (without it the healthcheck can't reach the master), path-style access, and an
explicit region in every client. MinIO is not used: its community edition is archived.

### 3.4 Iceberg REST catalog

**Code:** [docker-compose.yml#L39-L55](../infra/docker-compose.yml#L44-L60),
[docker/iceberg-rest.Dockerfile](../docker/iceberg-rest.Dockerfile).

One catalog serves every engine: PyIceberg ([catalog.py](../src/lakehouse/iceberg/catalog.py#L8)) and Spark
([common.py#L21-L22](../spark_jobs/common.py#L21-L22)) both talk to `http://iceberg-rest:8181`. The catalog is a
`JdbcCatalog` whose state lives in the Postgres database `iceberg_catalog`
([L50](../infra/docker-compose.yml#L50)).

**Why Postgres and not SQLite:** with SQLite, two streaming queries plus compaction committing at once failed with
`SQLITE_BUSY`, and after adding WAL with `SQLITE_BUSY_SNAPSHOT`. The stream crashed with `CommitStateUnknownException`.
The catalog uses optimistic concurrency (compare-and-swap on the metadata pointer), which Postgres row locking handles
correctly.

**Verify:** `curl localhost:8181/v1/namespaces/silver/tables`; the tables survive `make down && make up`.

### 3.5 PostgreSQL warehouse

**Code:** [infra/postgres/init/00_init.sh](../infra/postgres/init/00_init.sh) (first boot),
[sql/databases.sql](../infra/postgres/init/sql/databases.sql) (databases + owner roles),
[sql/warehouse.sql](../infra/postgres/init/sql/warehouse.sql) (everything inside `warehouse`). All of it is idempotent,
and `make init` re-applies it.

| Database | Owner | Holds |
|---|---|---|
| `warehouse` | postgres | `ops`, `lake_silver`, `staging`, `intermediate`, `marts`, `agent` |
| `airflow` | airflow | Airflow metadata DB |
| `iceberg_catalog` | iceberg | Iceberg JdbcCatalog tables |

Roles and least privilege ([warehouse.sql#L6-L19](../infra/postgres/init/sql/warehouse.sql#L6-L19),
[grants L199-L208](../infra/postgres/init/sql/warehouse.sql#L204-L213)):

| Role | Can do |
|---|---|
| `etl_writer` | owns `ops` and `lake_silver`; every pipeline task connects as this role |
| `dbt` | reads `lake_silver` and `ops`; owns `staging`, `intermediate`, `marts` |
| `grafana_ro`, `agent_ro` | read `ops` and `marts` only (default privileges cover future tables) |
| `agent_rw` | owns `agent` (pgvector tables, next stage) |

**`ops` tables** (written by every job):

| Table | Line | One row per |
|---|---|---|
| `ops.pipeline_run` | [L25](../infra/postgres/init/sql/warehouse.sql#L25) | task invocation: status, row counts, scrubbed error, Airflow run id |
| `ops.watermark` | [L44](../infra/postgres/init/sql/warehouse.sql#L44) | `(source, entity)` → last value loaded |
| `ops.reconciliation` | [L52](../infra/postgres/init/sql/warehouse.sql#L52) | layer hop: count_from/to, missing, duplicates, late, `ok`/`mismatch` |
| `ops.stream_metrics` | [L69](../infra/postgres/init/sql/warehouse.sql#L69) | micro-batch: events, late events, parse errors, max event lag |
| `ops.incident` | [L82](../infra/postgres/init/sql/warehouse.sql#L82) | failed task (from `on_failure_callback`) |
| `ops.dbt_result` | [L100](../infra/postgres/init/sql/warehouse.sql#L100) | dbt node result per invocation |

**`lake_silver` tables** are the copy targets, each with a primary key that matches the Silver unique key
([L113](../infra/postgres/init/sql/warehouse.sql#L113)). `financial_facts` uses
`unique nulls not distinct` ([L170](../infra/postgres/init/sql/warehouse.sql#L170)): `period_start` is null for
balance-sheet facts, and an ordinary unique constraint treats nulls as distinct, so it would never conflict and every
rerun would insert duplicates.

**Migration block** ([L193](../infra/postgres/init/sql/warehouse.sql#L193)): a guarded `do $$ … $$` block dropped the
four Tiingo-only price columns. It runs only while the old column exists, and drops the one dependent dbt view
explicitly instead of using `CASCADE`.

### 3.6 Configuration and secrets

**Code:** [src/lakehouse/config.py](../src/lakehouse/config.py#L9), [.env.example](../.env.example).

`Settings` (pydantic-settings) reads environment variables. Defaults are the in-container hostnames, so the same code
runs in every container. Secrets are `SecretStr`, which prevents them from being printed by accident.
`settings()` is cached ([L53](../src/lakehouse/config.py#L53)). `.env` is git-ignored. The only external credential
left is `SEC_USER_AGENT`; yfinance and Binance need none.

---

## 4. Shared Python package `lakehouse`

Code shared by Airflow tasks, Spark drivers and scripts: [src/lakehouse/](../src/lakehouse/).

### 4.1 Telemetry: `@instrumented`, reconciliation, watermarks, incidents

**Code:** [src/lakehouse/ops/telemetry.py](../src/lakehouse/ops/telemetry.py),
[src/lakehouse/ops/scrub.py](../src/lakehouse/ops/scrub.py).

**`@instrumented(source, entity)`** ([L91-L112](../src/lakehouse/ops/telemetry.py#L91-L112)) wraps a task body. It
inserts a `running` row, runs the function, then marks the row `success` (with any `rows_read`/`rows_written`/
`rows_quarantined` the function returned) or `failed` (with the exception class and a scrubbed message):

```python
run_id = uuid.uuid4().hex
_run_started(run_id, airflow_context(), source or kwargs.get("source"), entity or kwargs.get("entity"), logical_date)
try:
    result = fn(*args, **kwargs)
except BaseException as exc:
    _run_finished(run_id, "failed", {}, exc)
    raise
_run_finished(run_id, "success", result if isinstance(result, dict) else {}, None)
```

- **Fail-open:** `_run_started`, `_run_finished`, `record_reconciliation` and `on_failure_callback` are wrapped by
  [`_fail_open`](../src/lakehouse/ops/telemetry.py#L29). An `ops` write error is logged and swallowed, so telemetry
  can never fail the ETL.
- **Watermarks are deliberately *not* fail-open** ([L145](../src/lakehouse/ops/telemetry.py#L145)): they decide
  *what* gets loaded, so a failure must stop the task. A unit test asserts both behaviors
  ([tests/unit/test_telemetry.py](../tests/unit/test_telemetry.py)).
- **Airflow context** ([`airflow_context`](../src/lakehouse/ops/telemetry.py#L41)) comes from the Task SDK inside a
  task. Inside `spark-submit` subprocesses it comes from the `AIRFLOW_CTX_*` env vars that BashOperator exports, which
  is how Spark jobs get the right dag/task/run ids.
- **Reconciliation status** is `ok` only if counts match and there are no missing or duplicate rows
  ([L131](../src/lakehouse/ops/telemetry.py#L131)).
- **`on_failure_callback`** ([L164](../src/lakehouse/ops/telemetry.py#L164)) inserts an `ops.incident` and posts to
  `ALERT_WEBHOOK_URL` if one is set. It is attached to every task through `DEFAULT_ARGS`.

**Scrubber** ([scrub.py#L11](../src/lakehouse/ops/scrub.py#L11)) masks `token=…`-style secrets, quoted literals, long
tokens (≥ 32 chars) and digit runs (≥ 6), then truncates to 2,000 characters. Every error message stored in `ops` goes
through it, because the triage agent (next stage) will read `ops` and send it to an LLM.

### 4.2 Iceberg table definitions and schema evolution

**Code:** [src/lakehouse/iceberg/tables.py](../src/lakehouse/iceberg/tables.py),
[catalog.py](../src/lakehouse/iceberg/catalog.py), run by [scripts/init_lake.py](../scripts/init_lake.py).

All schemas and partition specs live in **one dict**, `TABLES` ([L57](../src/lakehouse/iceberg/tables.py#L57)).
Shared column groups:

- `META` = `_run_id, _source, _ingested_at, _payload_hash` (every table)
- `BATCH_META` = `META + _logical_date` (batch Bronze: the partition key that makes reruns replace themselves)
- `SILVER_META` = `META + _updated_at` (drives the incremental copy to Postgres)

| Table | Partitioning | Why |
|---|---|---|
| `bronze.prices_eod_raw`, `bronze.sec_companyfacts_raw`, `bronze.quarantine` | `identity(_logical_date)` | a rerun overwrites exactly its own partition |
| `bronze.crypto_trades` | `day(trade_time)`, `bucket(4, symbol)` | time pruning plus spreading hot symbols |
| `bronze/silver.equity_bars_1m`, `silver.crypto_bars_1m` | `day(minute)` | time pruning for 15-minute copies |
| `silver.crypto_trades` | `day(trade_time)` | the dedup MERGE reads only a trailing window |
| `silver.prices_eod`, `silver.financial_facts` | none | 15k / 152k rows; partitions would only create tiny files |

[`create_all`](../src/lakehouse/iceberg/tables.py#L110) creates missing tables and calls
[`sync_schema`](../src/lakehouse/iceberg/tables.py#L95) on existing ones. Columns no longer defined are dropped and new
ones added, through `table.update_schema()` ([L102-L106](../src/lakehouse/iceberg/tables.py#L102-L106)). In Iceberg
this is a metadata-only change: no data file is rewritten, and column ids are never reused, so old files stay readable.
That is how the Tiingo-only columns were removed (`dropped ['adj_high', 'adj_low', 'adj_open', 'adj_volume']`).

### 4.3 Universe

**Code:** [src/lakehouse/universe.py](../src/lakehouse/universe.py), data in
[dbt/seeds/universe.csv](../dbt/seeds/universe.csv). It is the single source of truth for tickers, CIKs, sectors and
`prior_ciks`. DAGs, the equity producer (the CSV is mounted read-only) and dbt (as a seed) all read the same file.

---

## 5. Batch ingestion: source → Bronze

### 5.1 Contracts (validation at the edge)

**Code:** [src/lakehouse/ingestion/contracts.py](../src/lakehouse/ingestion/contracts.py).

| Contract | Line | Validates |
|---|---|---|
| `YahooDailyRow` | [L11](../src/lakehouse/ingestion/contracts.py#L11) | yfinance daily record: positive prices, `high ≥ low`, int volume, `Stock Splits` 0.0 → 1.0 |
| `CompanyFactsEnvelope` | [L33](../src/lakehouse/ingestion/contracts.py#L33) | EDGAR top level: `cik`, `entityName`, `facts` containing `us-gaap` |
| `YahooMinuteBar` | [L49](../src/lakehouse/ingestion/contracts.py#L49) | the message the equity producer publishes (tested against producer output) |
| `BinanceTrade` | [L64](../src/lakehouse/ingestion/contracts.py#L64) | inner `data` of a Binance trade event |

All were written *after* recording real responses into [tests/fixtures/](../tests/fixtures/). For example, the Binance
fixture showed that the `b`/`a` order-id fields no longer exist, and the yfinance fixture showed that `Close` is already
split-adjusted.

### 5.2 HTTP helper (EDGAR)

**Code:** [src/lakehouse/ingestion/http.py](../src/lakehouse/ingestion/http.py). [`get`](../src/lakehouse/ingestion/http.py#L34)
retries 429/5xx/connection errors with exponential backoff (tenacity, 5 attempts). A thread-safe
[`RateLimiter`](../src/lakehouse/ingestion/http.py#L18) keeps EDGAR at 8 req/s, under SEC's 10 req/s limit.

### 5.3 yfinance extractor

**Code:** [src/lakehouse/ingestion/yahoo.py](../src/lakehouse/ingestion/yahoo.py).

- [`fetch_daily`](../src/lakehouse/ingestion/yahoo.py#L42) calls
  `Ticker.history(interval="1d", auto_adjust=False, actions=True)`. yfinance's `end` is exclusive, hence `end + 1 day`
  ([L45](../src/lakehouse/ingestion/yahoo.py#L45)). It retries `YFRateLimitError` with backoff.
- `yf.config.debug.hide_exceptions = False` ([L24](../src/lakehouse/ingestion/yahoo.py#L24)) makes yfinance *raise*
  instead of printing an error and returning an empty DataFrame. Otherwise failures would look like "no data".
- [`to_records`](../src/lakehouse/ingestion/yahoo.py#L28) turns the DataFrame into JSON-safe dicts with yfinance's own
  column names. The index is formatted with `strftime("%Y-%m-%d")` in its New York time zone. Converting through UTC
  would shift midnight-ET dates to the previous day.
- [`parse_daily`](../src/lakehouse/ingestion/yahoo.py#L51) checks the column set first. **Any drift quarantines every
  row**, so a renamed column is never half-parsed. It then validates each record; the raw record is kept as `_raw_line`.

### 5.4 EDGAR extractor

**Code:** [src/lakehouse/ingestion/edgar.py](../src/lakehouse/ingestion/edgar.py). The User-Agent comes from env and
is required ([`_headers`](../src/lakehouse/ingestion/edgar.py#L16)).
[`latest_periodic_filing_date`](../src/lakehouse/ingestion/edgar.py#L36) reads `filings.recent` to decide whether to
download the multi-MB `companyfacts` payload at all.
[`validate_companyfacts`](../src/lakehouse/ingestion/edgar.py#L43) checks only the envelope: the nested facts are
validated by Spark's explicit schema, not in Python.

### 5.5 Bronze writer: atomic slice overwrite

**Code:** [src/lakehouse/ingestion/bronze_writer.py](../src/lakehouse/ingestion/bronze_writer.py).

[`overwrite_slice`](../src/lakehouse/ingestion/bronze_writer.py#L28) is the idempotency mechanism for batch Bronze:

```python
with table.transaction() as tx:
    tx.delete(slice_filter(source, logical_date, **in_filters))   # (_source, _logical_date[, cik IN (...)])
    if rows:
        tx.append(data)
```

The delete and the append are **one Iceberg snapshot**, so readers never see a half-replaced slice. Rerunning the same
logical date replaces the slice instead of appending. The optional `in_filters` narrow the slice, for example to the
CIKs fetched in this run ([tasks.py#L121](../src/lakehouse/ingestion/tasks.py#L127)).
[`write_quarantine`](../src/lakehouse/ingestion/bronze_writer.py#L45) uses the same mechanism for `bronze.quarantine`.
[`slice_keys`](../src/lakehouse/ingestion/bronze_writer.py#L54) reads only key columns, so reconciliation never scans
multi-MB payload columns.

### 5.6 Task bodies (extract → land → reconcile)

**Code:** [src/lakehouse/ingestion/tasks.py](../src/lakehouse/ingestion/tasks.py). Every function here is
`@instrumented`.

| Function | Line | Key behavior |
|---|---|---|
| `prices_window` | [L34](../src/lakehouse/ingestion/tasks.py#L34) | `[ds − 7 d, ds]`: deterministic, so a rerun fetches the same range; the overlap picks up vendor corrections |
| `extract_prices` | [L43](../src/lakehouse/ingestion/tasks.py#L43) | one ticker → `data/raw/<source>/<ds>/<ticker>.json` |
| `land_prices_bronze` | [L67](../src/lakehouse/ingestion/tasks.py#L67) | quarantine first, then the **> 5 % quarantine rate check** ([L72](../src/lakehouse/ingestion/tasks.py#L72)), and only then the Bronze overwrite: a 100 %-quarantined run (schema drift) fails without wiping its slice, and the rejected rows stay inspectable. A provisional latest bar (no close yet) is skipped, not quarantined |
| `reconcile_prices_source_bronze` | [L81](../src/lakehouse/ingestion/tasks.py#L81) | parsed keys vs Bronze keys → missing / duplicates |
| `extract_edgar` | [L96](../src/lakehouse/ingestion/tasks.py#L96) | skip unless a newer periodic filing exists ([L99-L100](../src/lakehouse/ingestion/tasks.py#L99-L100)) |
| `land_edgar_bronze` | [L108](../src/lakehouse/ingestion/tasks.py#L108) | overwrite **only the fetched CIKs**; watermark advanced per CIK after a successful land ([L130](../src/lakehouse/ingestion/tasks.py#L130)) |
| `reconcile_edgar_source_bronze` | [L135](../src/lakehouse/ingestion/tasks.py#L135) | fetched CIKs vs Bronze rows for those CIKs |

**Why mapped extract tasks plus a single land task:** 12 tasks committing to the same Iceberg partition would race.
Extracts run in parallel (max 4) and only write files; one task does the single commit.

---

## 6. Spark batch jobs: Bronze → Silver

### 6.1 Shared session

**Code:** [spark_jobs/common.py](../spark_jobs/common.py).
[`spark_session`](../spark_jobs/common.py#L14) configures the Iceberg SQL extensions and catalog `lake` = REST catalog
with S3FileIO on SeaweedFS ([L21-L22](../spark_jobs/common.py#L21-L22)). It also sets AQE on
([L30](../spark_jobs/common.py#L30)), the session time zone to UTC, and a laptop-sized `shuffle.partitions = 16`
([L33](../spark_jobs/common.py#L33)). [`latest_per_key`](../spark_jobs/common.py#L51) is the shared dedup:
`row_number()` over the key, ordered by recency. It is built-in Spark only, with no Python UDF.

Batch jobs run as `spark-submit --master local[*] --driver-memory 2g` inside the Airflow container
([dags/common.py#L23](../dags/common.py#L24)).

### 6.2 Prices: `bronze_to_silver_prices.py`

**Code:** [spark_jobs/bronze_to_silver_prices.py](../spark_jobs/bronze_to_silver_prices.py).

```sql
merge into lake.silver.prices_eod t
using src s
on t.ticker = s.ticker and t.trade_date = s.trade_date
when matched and s._ingested_at >= t._ingested_at and s._payload_hash <> t._payload_hash
  then update set *
when not matched then insert *
```

- The source is **only the run's Bronze slice** (filter on `_source` and `_logical_date`), never a full scan.
- `s._ingested_at >= t._ingested_at`: an older backfill rerun can't overwrite newer data.
- `s._payload_hash <> t._payload_hash`: an unchanged re-land is a no-op and does **not** bump `_updated_at`, so the
  downstream copy and dbt also see nothing new.
- Reconciliation inside the job: distinct Bronze keys vs Silver rows for those keys (semi-join). Bronze duplicates are
  reported as `duplicates`.

### 6.3 Facts: `bronze_to_silver_facts.py`

**Code:** [spark_jobs/bronze_to_silver_facts.py](../spark_jobs/bronze_to_silver_facts.py).

- An explicit nested schema `PAYLOAD → facts: map<taxonomy, map<concept, {label, units: map<unit, array<FACT>>}>>`
  ([L23](../spark_jobs/bronze_to_silver_facts.py#L23)) matches the recorded fixture.
- [`explode_facts`](../spark_jobs/bronze_to_silver_facts.py#L40) uses four built-in `explode`s
  ([L50-L53](../spark_jobs/bronze_to_silver_facts.py#L50-L53)), keeps `us-gaap` and `period_end ≥ 2019-01-01`, and
  computes a row-level hash of the business fields ([L66](../spark_jobs/bronze_to_silver_facts.py#L66)).
- The MERGE key `(cik, concept, unit, period_start, period_end, accession_no)` uses **null-safe equality**
  `<=>` ([L89](../spark_jobs/bronze_to_silver_facts.py#L89)), because `period_start` is null for instant facts.
- Reconciliation counts duplicate *payloads* in Bronze. Repeated facts inside one EDGAR payload are a source quirk,
  handled by the dedup, not a load error.

### 6.4 Crypto Silver dedup: `silver_crypto_trades.py`

**Code:** [spark_jobs/silver_crypto_trades.py](../spark_jobs/silver_crypto_trades.py). Every 15 minutes it reprocesses
the trailing **6 h** of Bronze ([L21-L22](../spark_jobs/silver_crypto_trades.py#L21-L22)). It MERGEs `when not
matched then insert` on `(symbol, trade_id)`, with a time predicate in the `on` clause so Silver files outside the
window are pruned. The 6-hour lookback means trades that landed late after a stream outage still reach Silver.

### 6.5 Maintenance: `maintenance.py`

**Code:** [spark_jobs/maintenance.py](../spark_jobs/maintenance.py). It runs Iceberg procedures:
`rewrite_data_files` (compaction to 128 MB targets, `partial-progress.enabled`) on the five streaming tables
([L30](../spark_jobs/maintenance.py#L30)), `expire_snapshots` older than 7 days but keeping 5
([L32](../spark_jobs/maintenance.py#L32)), and `remove_orphan_files` with `prefix_listing => true`
([L35](../spark_jobs/maintenance.py#L35)), which lists files through S3FileIO so the Airflow image needs no Hadoop S3
filesystem. The stream keeps committing meanwhile, so a maintenance commit can lose the optimistic-concurrency race.
Each table and procedure is therefore isolated, and the run fails once at the end with a precise list
(`MaintenanceFailed`). Measured: 30 → 3, 72 → 1 and 68 → 1 files while the stream kept committing.

---

## 7. Streaming: producers and the Spark stream app

### 7.1 Binance producer

**Code:** [streaming/producer.py](../streaming/producer.py). It subscribes to the combined trade stream for 3 pairs and
forwards the inner `data` object **unchanged** to topic `crypto_trades` (6 partitions, key = symbol,
[L46](../streaming/producer.py#L46)). The producer uses `acks=all` and `enable.idempotence=True`
([L57](../streaming/producer.py#L57)), so broker retries don't create duplicates. On any disconnect it reconnects with
exponential backoff plus jitter. Parsing is left to Spark, so the contract lives in one place.

Limitation: a WebSocket can't be replayed. Trades that happen while the producer is down are lost at the source
(measured during stack restarts). That's why `make stream-stop` stops only Spark.

### 7.2 Delayed equity producer (simulated free-tier feed)

**Code:** [streaming/equity_producer.py](../streaming/equity_producer.py).

- Every `POLL_SECONDS` (60) it requests 1-minute history for the 12 equities, one request per ticker.
- [`releasable`](../streaming/equity_producer.py#L54) is the embargo rule:

  ```python
  if minute + timedelta(minutes=1) + DELAY > now or (last_sent and minute <= last_sent):
      continue
  ```

  A bar is released only when it is **complete** and **at least `QUOTE_DELAY_MINUTES` (default 10) old**, and only
  once per process.
- On (re)start the first poll uses `STARTUP_PERIOD = 5d` ([L81](../streaming/equity_producer.py#L81)), then switches
  to `1d` ([L106](../streaming/equity_producer.py#L106)). **This is how it refills its own gap:** Yahoo keeps days of
  1-minute history, so after downtime the missing bars are simply re-read.
- Messages carry `published_at` and `delay_minutes`, so the delay can be measured downstream
  (`stg_equity_bars_1m.delay_seconds`).
- It is self-contained: it doesn't import the `lakehouse` package (the producer image doesn't ship it). It reads the
  universe CSV mounted read-only.

### 7.3 Spark stream app: `stream_market.py` (three queries, one app)

**Code:** [spark_jobs/stream_market.py](../spark_jobs/stream_market.py). It runs on the Spark image's **Python 3.10**,
so it uses `timezone.utc` (ruff UP017 is disabled for this file only).

[`main`](../spark_jobs/stream_market.py#L223) builds one session with `spark.sql.caseSensitive=true`
([L228](../spark_jobs/stream_market.py#L228)), because Binance fields differ only by case (`t`/`T`, `e`/`E`,
`m`/`M`). It also configures `s3a` for checkpoints, then starts:

| Query | Lines | Source → sink | Semantics |
|---|---|---|---|
| **A `bronze_trades`** | [L247-L248](../spark_jobs/stream_market.py#L247-L248) | `crypto_trades` → [`BronzeSink`](../spark_jobs/stream_market.py#L115): good rows → `bronze.crypto_trades`, bad → `bronze.quarantine`, metrics → `ops.stream_metrics` | foreachBatch, at-least-once on replay |
| **B `bars_1m`** | [L249-L251](../spark_jobs/stream_market.py#L249-L251) | `crypto_trades` → [`to_bars`](../spark_jobs/stream_market.py#L97) → `silver.crypto_bars_1m` | `withWatermark("trade_time", "2 minutes")` + 1-minute tumbling window, append mode (each window emitted once, after the watermark passes it) |
| **C `equity_bars`** | [L252-L253](../spark_jobs/stream_market.py#L252-L253) | `equity_bars_1m` → [`EquitySink`](../spark_jobs/stream_market.py#L155): Bronze append, Silver MERGE, metrics | foreachBatch; MERGE makes re-published bars harmless |

Details that matter:

- **Parsing:** [`parse`](../spark_jobs/stream_market.py#L52) and [`parse_equity`](../spark_jobs/stream_market.py#L76)
  apply an explicit schema with `from_json` in PERMISSIVE mode and compute a `_valid` flag. Invalid or unparseable
  records are quarantined, never dropped.
- **Offsets:** `startingOffsets=earliest` applies only to the first start ([L241](../spark_jobs/stream_market.py#L241));
  afterwards the checkpoint under `s3a://lake/checkpoints/v2/<query>` wins.
- **Equity Silver hash** ([L179](../spark_jobs/stream_market.py#L179)) covers bar values only. A re-published bar has a
  new `published_at` but the same values, so it isn't counted as an update.
- **Late events:** a record older than (max event time seen − 2 min) ([L140](../spark_jobs/stream_market.py#L140),
  [L194](../spark_jobs/stream_market.py#L194)). The max is tracked on the driver and resets on restart (documented
  shortcut).
- **Metrics writer** [`write_metrics`](../spark_jobs/stream_market.py#L209) is fail-open, so the stream never stops
  because `ops` is unreachable.

**Verify:** `python scripts/check_stream_continuity.py` (inside the airflow container). Binance trade ids are
sequential, so `missing = max − min + 1 − distinct` is exact.

---

## 8. Silver → Postgres copy

**Code:** [src/lakehouse/warehouse_copy.py](../src/lakehouse/warehouse_copy.py).

[`copy_table`](../src/lakehouse/warehouse_copy.py#L49), per table:

1. Reads the watermark `("lake_silver", table)`, then scans Silver with
   `_updated_at > watermark − 5 min` ([L51](../src/lakehouse/warehouse_copy.py#L51)). The 5-minute overlap protects
   against commit-order races; the upsert makes it harmless.
2. Selects exactly the Postgres columns (read from `information_schema`), so the explicit column list always matches.
3. Inside one transaction: `create temp table tmp_copy (like …) on commit drop`
   ([L61](../src/lakehouse/warehouse_copy.py#L61)), a `COPY` into it, then
   [`upsert_sql`](../src/lakehouse/warehouse_copy.py#L34): `insert … select … on conflict (keys) do update set
   <non-key cols>`. `financial_facts` conflicts on its `nulls not distinct` constraint instead
   ([L22](../src/lakehouse/warehouse_copy.py#L22)).
4. Advances the watermark to the max `_updated_at` copied and writes a silver → postgres reconciliation row.

**Why not Spark JDBC:** about 150k rows at most; PyIceberg avoids a 20–30 s JVM start and a JDBC driver, and `COPY`
is Postgres's fastest load path.

---

## 9. Orchestration: Airflow DAGs

Airflow 3.3 (`airflow standalone`, LocalExecutor, Postgres metadata DB). DAG files are thin: they only wire functions
from `src/` and `spark-submit` calls. Shared settings live in [dags/common.py](../dags/common.py): 3 retries with
exponential backoff, `on_failure_callback` → incident ([L10](../dags/common.py#L10)), and the Assets
([L18-L21](../dags/common.py#L18-L21)). All DAGs start paused.

### 9.1 `prices_eod_daily` and `backfill_prices`

**Code:** [dags/prices_eod.py](../dags/prices_eod.py). One builder function
[`build`](../dags/prices_eod.py#L14) produces both DAGs, so they can't drift apart:

```
tickers ──> extract_prices.expand(ticker) ──> validate_and_land_bronze ──> reconcile_source_bronze
        ──> spark_bronze_to_silver_prices (BashOperator) ──> copy_silver_to_pg  (outlet: Asset silver_prices)
```

- Daily: `0 23 * * 1-5`, `_source = yfinance` ([L59](../dags/prices_eod.py#L59)).
- Backfill: manual, `params.start/end`, `_source = yfinance_backfill` ([L62](../dags/prices_eod.py#L62)). It's a
  separate source so a daily rerun on the same logical date can never overwrite 5 years of history.
- `max_active_tis_per_dagrun=4` ([L22](../dags/prices_eod.py#L22)) keeps Yahoo requests gentle.

### 9.2 `sec_facts_weekly`

**Code:** [dags/sec_facts_weekly.py](../dags/sec_facts_weekly.py). It runs Saturdays at 02:00 with the same shape as
prices. [`companies`](../dags/sec_facts_weekly.py#L20) expands each ticker into its current CIK **plus `prior_ciks`**
([L21-L24](../dags/sec_facts_weekly.py#L21-L24)). XOM re-registered in 2026, and its history lives under the old CIK.

### 9.3 `stream_silver_15m`

**Code:** [dags/stream_silver_15m.py](../dags/stream_silver_15m.py). Every 15 minutes:
`spark_dedup_trades → copy_bars_to_pg → check_stream_health`, plus `copy_equity_bars_to_pg` in parallel.
[`check_stream_health`](../dags/stream_silver_15m.py#L39) fails with `StreamStalled` if `ops.stream_metrics` has no
crypto events in the last 5 minutes. That's how Airflow *monitors* a stream it doesn't run. The equity feed is excluded
because it is silent outside market hours.

### 9.4 `dbt_build`

**Code:** [dags/dbt_build.py](../dags/dbt_build.py). Scheduled by `AssetOrTimeSchedule`
([L18](../dags/dbt_build.py#L18)): it runs when `silver_prices` or `silver_facts` is updated, **or** hourly (for the
stream bars). There are no sensors. Tasks: `dbt source freshness` → `dbt build` →
[`publish_dbt_results`](../dags/dbt_build.py#L28) with `trigger_rule="all_done"`, so results are published even when
the build fails. Parsing is in [src/lakehouse/dbt_results.py](../src/lakehouse/dbt_results.py#L10) →
`ops.dbt_result`.

### 9.5 `iceberg_maintenance`

**Code:** [dags/iceberg_maintenance.py](../dags/iceberg_maintenance.py). Daily at 03:00, it runs
[maintenance.py](../spark_jobs/maintenance.py). This is the only place compaction runs; the stream job never compacts.

---

## 10. Gold layer: dbt

**Code:** [dbt/](../dbt/). Profiles in [dbt/profiles.yml](../dbt/profiles.yml) (targets `dev` and `ci`), project config
in [dbt_project.yml](../dbt/dbt_project.yml). A [`generate_schema_name`](../dbt/macros/generate_schema_name.sql)
override makes models land in the pre-created `staging` / `intermediate` / `marts` schemas, which already have grants.
dbt-core 1.12.7, dbt-postgres 1.11.0, dbt_utils 1.4.1.

### 10.1 Sources and freshness

[models/staging/_sources.yml](../dbt/models/staging/_sources.yml):

| Source | Freshness | Why |
|---|---|---|
| `prices_eod` | warn 26 h, **error 74 h** on `_updated_at` ([L10-L14](../dbt/models/staging/_sources.yml#L10-L14)) | 74 h covers a normal Fri 23:00 → Mon 23:00 weekend |
| `financial_facts` | none | rows change only when a company files (quarterly) |
| `equity_bars_1m` | none | silent outside market hours |
| `crypto_bars_1m` | warn 30 min, error 120 min ([L23-L26](../dbt/models/staging/_sources.yml#L23-L26)) | 24/7 market |

### 10.2 Staging (views)

[`stg_prices_eod`](../dbt/models/staging/stg_prices_eod.sql), [`stg_financial_facts`](../dbt/models/staging/stg_financial_facts.sql)
(adds `period_type` and `duration_days`), [`stg_crypto_bars_1m`](../dbt/models/staging/stg_crypto_bars_1m.sql)
(`symbol` → `ticker`), [`stg_equity_bars_1m`](../dbt/models/staging/stg_equity_bars_1m.sql) (adds `delay_seconds`) and
[`stg_universe`](../dbt/models/staging/stg_universe.sql) (seed). Explicit column lists; no `select *`.

### 10.3 Intermediate (tables)

| Model | What it does | Key line |
|---|---|---|
| [`int_daily_returns`](../dbt/models/intermediate/int_daily_returns.sql) | `ln(adj_close / lag(adj_close))` per ticker | [L6](../dbt/models/intermediate/int_daily_returns.sql#L6) |
| [`int_intraday_returns_1m`](../dbt/models/intermediate/int_intraday_returns_1m.sql) | 1-minute log returns for crypto and equities; equity `lag` is partitioned by New York session date, so the overnight gap is never a "1-minute" return | [L21](../dbt/models/intermediate/int_intraday_returns_1m.sql#L21) |
| [`int_fundamentals_quarterly`](../dbt/models/intermediate/int_fundamentals_quarterly.sql) | one row per **ticker** × fiscal period, metrics pivoted via [`concept_aliases.csv`](../dbt/seeds/concept_aliases.csv) | see below |

`int_fundamentals_quarterly` is the most subtle model:

1. A 10-Q also reports **prior-year comparatives under the filing's own `fy`/`fp`**. Only each filing's current period
   is kept: `period_end = max(period_end) over (partition by cik, accession_no)`
   ([L18-L24](../dbt/models/intermediate/int_fundamentals_quarterly.sql#L18-L24)).
2. Duration facts must match the period length: 80–100 days for Q1–Q3, 350–380 for FY
   ([L32-L33](../dbt/models/intermediate/int_fundamentals_quarterly.sql#L32-L33)). This drops year-to-date values.
3. The latest filing per `(ticker, fy, fp, metric)` wins, so a 10-Q/A restatement replaces the original; ties go to
   the lowest alias priority ([L40](../dbt/models/intermediate/int_fundamentals_quarterly.sql#L40)).
   `has_restatement` is set when the values differ across filings
   ([L43](../dbt/models/intermediate/int_fundamentals_quarterly.sql#L43)).
4. It groups by ticker, not CIK, so XOM's two CIKs form one series.

### 10.4 Marts

| Model | Grain | Logic (key lines) |
|---|---|---|
| [`dim_company`](../dbt/models/marts/dim_company.sql) | ticker | the universe |
| [`fct_rolling_volatility`](../dbt/models/marts/fct_rolling_volatility.sql) | ticker × day | **incremental merge.** `vol_20d`/`vol_60d` = `stddev_samp` over 20/60 rows × √252, null until the window is full ([L37-L38](../dbt/models/marts/fct_rolling_volatility.sql#L37-L38)); z-score vs the trailing 252 days ([L48](../dbt/models/marts/fct_rolling_volatility.sql#L48)) |
| [`fct_drawdown`](../dbt/models/marts/fct_drawdown.sql) | ticker × day | running max ([L6-L7](../dbt/models/marts/fct_drawdown.sql#L6-L7)), `adj_close / running_max − 1`, worst drawdown to date |
| [`fct_correlation_60d`](../dbt/models/marts/fct_correlation_60d.sql) | pair × day | self-join on date with `a.ticker < b.ticker`; Postgres `corr()` as a window function ([L19](../dbt/models/marts/fct_correlation_60d.sql#L19)) |
| [`fct_fundamentals_asof`](../dbt/models/marts/fct_fundamentals_asof.sql) | ticker × day | `left join lateral (… where filed_date <= trade_date order by filed_date desc limit 1)` ([L22](../dbt/models/marts/fct_fundamentals_asof.sql#L22)): **point-in-time, no look-ahead** |
| [`fct_intraday_volatility`](../dbt/models/marts/fct_intraday_volatility.sql) | ticker × minute | 60-minute stdev × √525,600 (crypto) or × √98,280 (equity), window reset each NY session ([L15-L29](../dbt/models/marts/fct_intraday_volatility.sql#L15-L29)) |
| [`mart_fact_sheet_inputs`](../dbt/models/marts/mart_fact_sheet_inputs.sql) | ticker × fiscal period | financials + QoQ/YoY + margins + debt/equity + period risk (avg/max vol, worst drawdown); feeds the agent fact sheets in the next stage |

**The incremental model in detail** (an interview favorite):

```sql
{% set output_lookback_days = 90 %}   -- rows rewritten each run (MERGE)
{% set input_lookback_days = 500 %}   -- history read so windows are complete
where trade_date >= (select coalesce(max(trade_date), date '1900-01-01') - {{ input_lookback_days }} from {{ this }})
```

The output window (90 days) is rewritten, but the *input* must reach much further back. A z-score over 252 trading
days of `vol_20d`, each built from 20 returns, needs ~395 calendar days of history before the oldest rewritten row.
The `coalesce` fixes a real bug: the model was first built when prices were empty, `max()` was null, and `>= null`
matched nothing, so the mart stayed empty forever while every test passed.

### 10.5 Tests, macros, seeds

- **Generic tests** in each layer's YAML: `dbt_utils.unique_combination_of_columns` on every fact, `not_null`,
  `accepted_values` (sector, asset class, fiscal period), `relationships` to `dim_company`, and
  `dbt_utils.accepted_range` (prices > 0, volatility ≥ 0, drawdown in [−1, 0], correlation in [−1, 1]).
- **Singular tests** in [dbt/tests/](../dbt/tests/):
  [`assert_no_lookahead`](../dbt/tests/assert_no_lookahead.sql) (`filed_date > trade_date` must return 0 rows),
  [`assert_returns_sane`](../dbt/tests/assert_returns_sane.sql) (|log return| ≤ 0.5),
  [`assert_no_trading_gaps`](../dbt/tests/assert_no_trading_gaps.sql) (warn on gaps > 4 days),
  [`assert_revenue_coverage`](../dbt/tests/assert_revenue_coverage.sql) (warn if < 80 % of periods have revenue,
  which means a concept alias is missing).
- **Macro** [`annualize_vol`](../dbt/macros/annualize_vol.sql): one definition of "× √periods". It must stay in sync
  with [METRICS.md](METRICS.md).
- **Seeds:** [`universe.csv`](../dbt/seeds/universe.csv), [`concept_aliases.csv`](../dbt/seeds/concept_aliases.csv)
  (concept, unit, metric, priority, period type).

---

## 11. Data browser

**Code:** [scripts/data_browser.py](../scripts/data_browser.py) · `make browse` → http://localhost:8501

A standard-library `http.server` app, with no new dependencies. Pages:

| URL | Shows | Function |
|---|---|---|
| `/` | every Postgres table with row counts; every Iceberg table with records, files, size | [`overview`](../scripts/data_browser.py#L62) |
| `/pg?schema=&table=` | columns and first rows | [`pg_table`](../scripts/data_browser.py#L100) |
| `/iceberg?table=ns.t` | schema, partition spec, last 10 snapshots, data files (linked), first rows | [`iceberg_table`](../scripts/data_browser.py#L115) |
| `/files?root=s3\|local&path=` | directory listing; Parquet preview (first rows); JSON/CSV first 64 KB | [`files`](../scripts/data_browser.py#L148) |

Safety: Postgres sessions are `default_transaction_read_only=on` with a 10 s statement timeout
([L41](../scripts/data_browser.py#L41)). Table names are checked against `information_schema` and a schema allowlist
before use ([L105](../scripts/data_browser.py#L105)). Local paths can't escape `data/`
([L151](../scripts/data_browser.py#L151)), which is also mounted read-only. The port is bound to `127.0.0.1` because the
page has no auth.

---

## 12. Tests, fixtures and CI

| Level | Where | Covers |
|---|---|---|
| Unit (no network, no Docker) | [tests/unit/](../tests/unit/) | contracts vs recorded fixtures, yfinance parse (incl. the real AAPL 4:1 split day), schema drift → quarantine, EDGAR helpers, scrubber, fail-open telemetry, upsert SQL rendering, the equity producer's delay rule |
| Spark unit (local SparkSession) | [tests/unit/spark/](../tests/unit/spark/) | stream parsers on real Binance/yfinance messages, 1-minute OHLCV/VWAP aggregation, `latest_per_key`, XBRL explode on the real Apple payload |
| Integration (live stack) | [tests/integration/test_iceberg_roundtrip.py](../tests/integration/test_iceberg_roundtrip.py) | PyIceberg create → Spark insert → PyIceberg read through the shared catalog (`make smoke`) |
| dbt | `dbt build` | 63 nodes (models, seeds, tests) |
| CI | [.github/workflows/ci.yml](../.github/workflows/ci.yml) | ruff + unit tests (Java 17 for Spark); `dbt build --target ci` against a pgvector service container initialized with `warehouse.sql` |

**Fixtures** in [tests/fixtures/](../tests/fixtures/) are real responses recorded by
[scripts/record_fixtures.py](../scripts/record_fixtures.py) (`make fixtures`). EDGAR payloads are trimmed to a few
concepts while keeping their structure. Tests that need a missing fixture *skip* rather than pass against a guess
([tests/unit/conftest.py](../tests/unit/conftest.py)).

---

## 13. Scripts and runbooks

| Script | Purpose |
|---|---|
| [scripts/init_lake.py](../scripts/init_lake.py) | create or evolve all Iceberg tables (part of `make init`) |
| [scripts/record_fixtures.py](../scripts/record_fixtures.py) | record real yfinance / EDGAR / Binance responses |
| [scripts/check_stream_continuity.py](../scripts/check_stream_continuity.py) | restart test: missing and duplicate trade ids per symbol in Bronze and Silver |
| [scripts/data_browser.py](../scripts/data_browser.py) | section 11 |

Runbooks in [runbooks/](../runbooks/) give, per alert, the signals, diagnosis queries and the fix. The DAG error
messages point to them, and they will be the triage agent's knowledge base:
[api_failure](../runbooks/api_failure.md), [stream_stalled](../runbooks/stream_stalled.md),
[reconciliation_mismatch](../runbooks/reconciliation_mismatch.md), [dbt_failure](../runbooks/dbt_failure.md).

---

## 14. Cross-cutting guarantees

### Idempotency, per hop

| Hop | Mechanism | Code |
|---|---|---|
| Source → Bronze (prices) | delete + append of `(_source, _logical_date)` in one commit; deterministic fetch window | [bronze_writer.py#L38](../src/lakehouse/ingestion/bronze_writer.py#L38), [tasks.py#L32](../src/lakehouse/ingestion/tasks.py#L34) |
| Source → Bronze (EDGAR) | same, scoped to the fetched CIKs; watermark skip | [tasks.py#L121](../src/lakehouse/ingestion/tasks.py#L127) |
| Kafka → Bronze (stream) | checkpointed offsets; foreachBatch may replay a batch (at-least-once) | [stream_market.py#L247](../spark_jobs/stream_market.py#L247) |
| Bronze → Silver | `MERGE` on the unique key; update only if the hash changed and the source is newer | §6.2–6.4, [stream_market.py#L187](../spark_jobs/stream_market.py#L187) |
| Silver → Postgres | watermark + overlap, `INSERT … ON CONFLICT DO UPDATE` | [warehouse_copy.py](../src/lakehouse/warehouse_copy.py) |
| Postgres → Gold | dbt tables rebuilt; incremental MERGE on `(ticker, trade_date)` | [fct_rolling_volatility.sql](../dbt/models/marts/fct_rolling_volatility.sql) |

### Unique keys

| Table | Key |
|---|---|
| prices_eod | `(ticker, trade_date)` |
| financial_facts | `(cik, concept, unit, period_start, period_end, accession_no)`, null-safe |
| crypto_trades | `(symbol, trade_id)` |
| crypto_bars_1m | `(symbol, minute)` |
| equity_bars_1m | `(ticker, minute)` |

### Quality checks and where they live

| Check | Where |
|---|---|
| Schema / types / required fields | pydantic contracts (batch), explicit Spark schema + `_valid` (stream) → `bronze.quarantine` |
| Quarantine rate > 5 % fails the run | [tasks.py `_check_quarantine_rate`](../src/lakehouse/ingestion/tasks.py#L59) |
| Counts, missing, duplicates per hop | `ops.reconciliation` |
| Stream health and lag | `ops.stream_metrics` + `check_stream_health` |
| Freshness | dbt sources |
| Keys, ranges, relationships, no look-ahead | dbt tests |

---

## 15. Measured results so far

| What | Result |
|---|---|
| Iceberg round trip PyIceberg ↔ Spark; catalog survives `down && up` | pass |
| Price backfill (5 y × 12 tickers) | 15,048 rows in 33 s; source → Bronze → Silver → Postgres all `ok` |
| Same logical date run twice | Bronze slice 72 → 72, Silver 15,048 → 15,048, Postgres `max(_updated_at)` unchanged |
| EDGAR load / rerun | 13 CIKs → 151,851 facts in 49 s; rerun fetched 0 CIKs, Bronze untouched |
| Spark stream SIGKILL for 60 s | 0 missing, 0 duplicate trade ids in Silver; Bronze replay count unchanged |
| Delayed equity feed | 12 bars/minute, ~661 s from bar start to publish |
| Equity producer stopped for 3 min mid-session | after restart: 0 missing minutes, 0 duplicates in Silver |
| Compaction during streaming | 30 → 3 files, no commit conflicts |
| dbt | 63 / 63 pass, 0 warnings on real data |
| Unit tests | 24 pass, 0 skipped |

---

## 16. Known limitations

- **yfinance** is an unofficial, scraped API (personal-use terms; it can throttle or change without notice).
- **Binance source gaps:** trades are lost while the *producer* is down (a WebSocket can't be replayed).
- **Late-event counting** resets on stream restart; the equity producer's 5-day refill shows up as late events.
- **Not yet done:** merge-on-read for `silver.financial_facts` with delete-file maintenance (PERFORMANCE.md); live-LLM
  evaluation numbers (need a DeepSeek key); README screenshots; the fresh-clone timing test.
- **Laptop simplifications:** single-node everything, `SIMPLE_AUTH_MANAGER_ALL_ADMINS` (the agent's Airflow token comes
  without credentials), a local staging folder instead of an object-store prefix, dbt on Postgres instead of on the lake,
  no auth on the localhost-only agents and browser ports.

---

## 17. Agents: the shared loop, the QA agent and the triage agent

**Code:** [agents/](../agents/) · container [docker/agents.Dockerfile](../docker/agents.Dockerfile) ·
service [agents/service.py](../agents/service.py)

There are two agents, and they share everything except their prompt and tools:

| Agent | Entry point | Tools (all read-only) |
|---|---|---|
| Financial QA | [`ask`](../agents/qa/agent.py#L64) (`make ask Q=...`, `POST /ask`) | `search_fact_sheets`, `list_tables`, `run_sql` ([L40](../agents/qa/agent.py#L40)) |
| Incident triage | [`triage`](../agents/triage/agent.py#L180) (`make triage INCIDENT=...`, `POST /triage`) | `get_incident`, `get_failed_runs`, `get_recent_runs`, `get_reconciliation_issues`, `get_stream_metrics`, `get_dbt_failures`, `get_task_log`, `search_runbooks` ([L146](../agents/triage/agent.py#L146)) |

### 17.1 The LLM client

[`DeepSeekClient.chat`](../agents/core/llm.py#L35) posts to `https://api.deepseek.com/chat/completions` with
`tools`, thinking mode enabled and a configurable `reasoning_effort`. It retries 429/5xx and timeouts with exponential
backoff. It uses plain `requests` rather than an SDK, because there is one endpoint.

### 17.2 The loop and its guardrails

[`run_agent`](../agents/core/loop.py#L69): reason → call tools → observe → repeat → final answer.

```
messages = [system, user]
loop:
    if steps / seconds / tokens over budget  -> forced final answer (tool_choice="none")
    reply = llm.chat(messages, tools)        # keeps reasoning_content (DeepSeek returns 400 otherwise)
    no tool calls?                           -> final answer
    for each tool call: guardrails -> run tool -> append {"role": "tool", ...}
    too many blocked calls                   -> forced final answer
```

| Guardrail | Where | Effect |
|---|---|---|
| step cap (`max_steps`) | [L148](../agents/core/loop.py#L148) | stops a model that never stops calling tools |
| time and token budgets | [L150-L152](../agents/core/loop.py#L150-L152) | bound cost and latency |
| repeat detection (`max_repeats`) | [L128](../agents/core/loop.py#L128) | the identical (tool, sorted-JSON args) call runs once; repeats get a "BLOCKED, reuse the result" observation |
| tool-call budget | [L136](../agents/core/loop.py#L136) | caps the total number of executed tools |
| blocked-call cap (`max_blocked`) | [L167](../agents/core/loop.py#L167) | a model that insists on looping is stopped |
| output truncation | [L144](../agents/core/loop.py#L144) | a huge tool result can't flood the context |
| forced final answer | [L108](../agents/core/loop.py#L108) | one last call with tools disabled, so a stopped run still answers, and `stopped_by` says why |
| errors as observations | inside `execute` | a failing tool, bad JSON or an unknown tool becomes a scrubbed `ERROR:` message the model can react to |
| audit log | [agents/core/trace.py](../agents/core/trace.py#L15) | every prompt, reply, tool call and result goes to `agent.run_log` (fail-open) |

The guardrails are tested with a scripted fake LLM ([tests/unit/test_agent_loop.py](../tests/unit/test_agent_loop.py)): a model
that repeats one call forever is stopped by `blocked_calls`, and one that never finishes is stopped by `max_steps`.

### 17.3 Retrieval: fact sheets, embeddings, hybrid search

- **Documents** ([agents/retrieval/documents.py](../agents/retrieval/documents.py)):
  [`fact_sheet`](../agents/retrieval/documents.py#L113) renders one Markdown sheet per row of
  `marts.mart_fact_sheet_inputs` (Jinja), with a JSON sidecar, into `data/fact_sheets/{ticker}/{fy}-{fp}.md`.
  Runbooks are documents too ([`runbook`](../agents/retrieval/documents.py#L130)). Each `## ` section is one chunk,
  prefixed with the document identity, so a retrieved chunk carries its citation id.
- **Store** ([warehouse.sql](../infra/postgres/init/sql/warehouse.sql#L240)): `agent.chunks` has
  `embedding vector(384)` with an HNSW index and a generated `tsvector` with a GIN index.
- **Incremental indexing** ([`refresh_index`](../agents/retrieval/search.py#L105)): only documents whose `content_hash`
  changed are re-embedded, and vanished documents are deleted. The `fact_sheets` DAG triggers it after every dbt build
  (Asset `marts`).
- **Hybrid search** ([`hybrid_search`](../agents/retrieval/search.py#L55)): pgvector cosine top-N plus full-text top-N
  with OR semantics ([`or_query`](../agents/retrieval/search.py#L49)), merged with
  [`rrf`](../agents/retrieval/search.py#L40). Optional filters: kind, ticker, fiscal year and period.
- **Evaluation** ([agents/eval/retrieval_eval.py](../agents/eval/retrieval_eval.py#L38),
  [qa_questions.yaml](../agents/eval/qa_questions.yaml)): recall@5 0.819 hybrid vs 0.764 vector vs 0.681 text.

### 17.4 The QA agent and its SQL guard

The prompt ([`SYSTEM`](../agents/qa/agent.py#L22)) requires answers only from tool results, with `[DOC-ID]` citations.
After the run, [`ask`](../agents/qa/agent.py#L64) compares the cited ids with the ids the tools actually returned
(`ungrounded_citations`, [L74](../agents/qa/agent.py#L74)).

[`check_sql`](../agents/qa/sql_guard.py#L31) is the NL-SQL guard. It parses with sqlglot and requires exactly one
statement, which must be a query. It rejects DML/DDL nodes anywhere in the tree
([`FORBIDDEN_NODES`](../agents/qa/sql_guard.py#L22)), tables outside the `marts` allowlist
([`ALLOWED_TABLES`](../agents/qa/sql_guard.py#L17)) and dangerous functions
([`FORBIDDEN_FUNC_PREFIXES`](../agents/qa/sql_guard.py#L24)). It wraps the query in a LIMIT.
[`run_sql`](../agents/qa/sql_guard.py#L59) then executes as `agent_ro`, read-only, with a 5 s timeout. 16 tests:
[tests/unit/test_sql_guard.py](../tests/unit/test_sql_guard.py).

### 17.5 The triage agent

The prompt ([`SYSTEM`](../agents/triage/agent.py#L31)) explains how the pipeline reports problems and fixes the note format:
Summary · Evidence · Likely cause · Confidence · Suggested next step · `CATEGORY:` (one of
[`CATEGORIES`](../agents/triage/agent.py#L26)). [`get_task_log`](../agents/triage/agent.py#L120) reads Airflow 3's
`/api/v2` logs, including `map_index` for mapped tasks and the structured `error_detail` that holds the exception,
and scrubs every line. The note goes to `incidents/<id>.md`. The agent can update only `ops.incident.agent_diagnosis`
and `triage_note_path`, through a column-level grant ([warehouse.sql](../infra/postgres/init/sql/warehouse.sql#L287)).

### 17.6 The agents service

[agents/service.py](../agents/service.py#L49) is a standard-library HTTP server holding the embedding model in memory:
`POST /fact-sheets/refresh`, `/ask`, `/triage`, and `/grafana-webhook`.
[`incidents_from_grafana`](../agents/service.py#L29) turns firing alerts into `ops.incident` rows; auto-triage is
optional (`TRIAGE_ON_ALERT`).

---

## 18. Grafana: dashboard and alerts as code

**Code:** [scripts/build_dashboard.py](../scripts/build_dashboard.py) → `infra/grafana/provisioning/` (`make dashboard`).

- **Datasource** ([warehouse.yaml](../infra/grafana/provisioning/datasources/warehouse.yaml)): Postgres as `grafana_ro`.
- **Dashboard** ([`dashboard`](../scripts/build_dashboard.py#L36)): 12 panels. Volatility 20d/60d, regime heatmap
  (status history), drawdown, latest z-scores, correlation matrix, intraday volatility, freshness, reconciliation, stream
  events and lag, dbt results.
- **Alerts** ([`alert_rules`](../scripts/build_dashboard.py#L109)): volatility spike (z > 3, one alert instance per ticker),
  missed daily load (weekend-aware, [L117](../scripts/build_dashboard.py#L117)), stream stalled, reconciliation mismatch,
  dbt failure, quarantine spike. Each has a severity label and a runbook link.
- **Routing** ([contact-points.yaml](../infra/grafana/provisioning/alerting/contact-points.yaml)): a webhook to the agents
  service, which opens `ops.incident`.
- **Incident dedupe** ([telemetry.py](../src/lakehouse/ops/telemetry.py#L177)): at most one open incident per failed DAG
  run, enforced by a partial unique index.

---

## 19. PII separation

**Code:** [agents/pii/subscribers.py](../agents/pii/subscribers.py),
[fct_alert_targets.sql](../dbt/models/marts/fct_alert_targets.sql),
[tests/integration/test_security.py](../tests/integration/test_security.py).

`pii.subscribers` holds synthetic name/email/phone data, readable only by `pii_admin` (`revoke all … from public`,
[warehouse.sql](../infra/postgres/init/sql/warehouse.sql#L289)). Analytics sees only
[`subscriber_key`](../agents/pii/subscribers.py#L28) = HMAC-SHA256(secret, id). `marts.fct_alert_targets` lists the keys
to alert today. [`notify`](../agents/pii/subscribers.py#L57), running as `pii_admin`, is the only join back to contacts,
and it logs masked emails. The integration tests prove that four roles get "permission denied" and that no
email/phone column exists outside `pii`.

---

## 20. Fault injection and the performance harness

- **[scripts/inject_fault.py](../scripts/inject_fault.py)** (`make fault FAULT=...`, `make fault-cleanup FAULT=...`)
  injects faults through the production paths:
  - `api_500` and `schema_change` run `prices_eod_daily` in-process with the source patched
    ([`run_prices_with_fault`](../scripts/inject_fault.py#L89)).
  - `duplicate_batch` appends a Bronze slice a second time, bypassing the overwrite
    ([`duplicate_slice`](../scripts/inject_fault.py#L114)), so reconciliation and the Grafana alert catch it.
  - `stream_stop` stops the producer.

  Each fault creates exactly one real incident, which [agents/eval/agent_eval.py](../agents/eval/agent_eval.py#L50)
  hands to the triage agent and scores.
- **[spark_jobs/perf_facts.py](../spark_jobs/perf_facts.py)** imports the production facts job. Each variant starts from
  the same snapshot ([`rollback_to_snapshot`](../spark_jobs/perf_facts.py#L68)) and is measured through the Spark REST
  API for its job group ([`stage_metrics`](../spark_jobs/perf_facts.py#L27)). Results and analysis:
  [PERFORMANCE.md](PERFORMANCE.md).
