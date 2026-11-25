# Project plan: Lakehouse for Market Volatility Monitoring and Financial AI Agents

Owner: Hieu Le Minh Phan · Written 2026-11-25 · Target: 7 working days

This is the full plan for the project. `CLAUDE.md` in the repo root tells Claude Code how to work. This file says **what** to build and **why**. Track progress in `docs/PROGRESS.md` (section 14 has the checklist to copy).

---

## 1. Goal

Build a small, realistic lakehouse that a MoMo interviewer would recognize as their own kind of work (JD 26-ITC-0757):

| JD responsibility | Where this project shows it |
|---|---|
| Batch + streaming ingestion into Apache Iceberg, safe to re-run | Airflow batch DAGs and a Kafka → Spark Structured Streaming job, both writing Iceberg Bronze tables idempotently |
| Schema validation, data quality, reconciliation against the source | pydantic validation with a quarantine table, `ops.reconciliation` per run, dbt tests and freshness |
| Tested dbt models from raw to marts | dbt staging → intermediate → marts with incremental models and tests |
| Make Spark jobs faster and cheaper | `docs/PERFORMANCE.md`: profiled baseline vs tuned job, with numbers |
| Keep personal data protected | Stretch: synthetic alert subscribers kept in a restricted `pii` schema, with only pseudonymous keys in analytics |
| Run what you ship (Airflow monitoring, alerts, runbooks) | `ops` schema, Grafana alerts, `runbooks/` |
| Automate data work with AI | Incident-triage agent, financial QA agent, optional natural-language SQL tool |

**Users of the output**
1. A risk analyst watching the **volatility dashboard** and its alerts.
2. **Financial AI agents** that answer questions from tested, agent-ready fact sheets.
3. The on-call data engineer (you), helped by the **triage agent**.

**Non-goals:** Kubernetes, cloud deployment, real-time trading signals, a production-grade auth setup, perfect XBRL coverage.

---

## 2. Architecture

```
 SOURCES                      INGESTION                     LAKEHOUSE (Iceberg on SeaweedFS S3, REST catalog)
 ────────                     ─────────                     ───────────────────────────────────────────────
 yfinance EOD prices (JSON)  ─┐
                               ├─ Airflow batch DAGs ──────> bronze.prices_eod_raw
 SEC EDGAR companyfacts (JSON) ┘  (validate, quarantine,     bronze.sec_companyfacts_raw
                                   watermark, idempotent)    bronze.quarantine
 Binance trade WebSocket ──> producer ──> Redpanda topic ──> Spark Structured Streaming
                                          crypto_trades       ──> bronze.crypto_trades
                                                              ──> silver.crypto_bars_1m (windowed agg)
                                                                       │
                                          PySpark batch (dedup, typing, explode XBRL, MERGE INTO)
                                                                       v
                                                              silver.prices_eod
                                                              silver.financial_facts
                                                              silver.crypto_trades
                                                                       │ incremental upsert (watermark)
                                                                       v
 WAREHOUSE (PostgreSQL 16 + pgvector)   lake_silver.*  ──> dbt: staging → intermediate → marts (gold)
                                                                       │
                     ┌─────────────────────────────┬───────────────────┴────────────────┐
                     v                             v                                    v
          Grafana dashboard + alerts     Fact sheets (MD + JSON) → agent.chunks   Triage agent
                                         → QA agent (hybrid search)               (ops, dbt results,
                                                                                    Airflow logs, runbooks)
 ops schema (Postgres): runs, watermarks, reconciliation, stream metrics, incidents  ← written by every job
```

**Design decisions (say these in interviews)**
- **Spark owns the lake layers, dbt owns business logic.** Bronze → Silver is file and table work (dedup, explode nested JSON, `MERGE INTO`), which suits Spark on Iceberg. Silver → Gold is metric logic that analysts should read and test, which suits dbt in SQL.
- **Silver is copied into Postgres** so that dbt, Grafana and pgvector use one engine on a laptop. In production you would run dbt on Spark or Trino directly against Iceberg. Say this honestly.
- **Bronze keeps the raw payload.** If an API changes or a parser has a bug, Silver can be rebuilt from Bronze without calling the API again.
- **One catalog (REST) for every engine.** PyIceberg and Spark see the same tables, the same way MoMo's engines share a catalog.
- **The streaming job runs as its own service, not inside Airflow.** Airflow schedules batch work. A long-running stream is a service that Airflow only monitors.

---

## 3. Stack and versions

Verify each version on day 1 and record the exact pins in `docs/VERSIONS.md`. Versions below were current as of late November 2026.

| Component | Choice | Notes |
|---|---|---|
| Python | 3.12 | |
| Orchestration | Apache Airflow **3.3.x** | Install with the official constraints file. Airflow 3 DAG imports come from `airflow.sdk`. For the laptop, run `airflow standalone` with LocalExecutor and a Postgres metadata DB. |
| Processing | Apache Spark **4.0.x** (Scala 2.13, Java 17) via `pyspark` | Local mode `local[*]` |
| Table format | Apache Iceberg **1.12.0**: `org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.12.0` and `org.apache.iceberg:iceberg-aws-bundle:1.12.0` | Iceberg 1.12 supports Spark 3.5, 4.0 and 4.1 |
| Kafka connector | `org.apache.spark:spark-sql-kafka-0-10_2.13:<same Spark version>` | Bake JARs into the image; don't use `--packages` at runtime |
| Python Iceberg client | PyIceberg ≥ 0.12 with `pyarrow`, `s3fs` extras | |
| Catalog | `apache/iceberg-rest-fixture` (JdbcCatalog on SQLite) | **Must persist:** set `CATALOG_URI=jdbc:sqlite:file:/catalog/iceberg.db` and mount `/catalog` as a volume. Fallback: Lakekeeper (Postgres-backed REST catalog). |
| Object storage | **SeaweedFS** (`chrislusf/seaweedfs`, `weed server -s3`) | MinIO community edition is archived and no longer ships images. Iceberg needs `s3.path-style-access=true` and an endpoint override. Fallback: Garage. |
| Streaming broker | Redpanda single node (`redpandadata/redpanda`) | Kafka API; much lighter than Kafka + ZooKeeper/KRaft |
| Warehouse | PostgreSQL 16 + pgvector (`pgvector/pgvector:pg16`) | Separate databases: `airflow`, `warehouse` |
| Transform | dbt-core + dbt-postgres (latest 1.x), `dbt_utils` | |
| Dashboard | Grafana OSS | Provisioned data source, dashboards and alerts as code |
| Agents | LangGraph + your existing Flask service; embeddings with `sentence-transformers` (e.g. `all-MiniLM-L6-v2`, 384 dims, runs locally) | LLM provider configurable via env |
| CI | GitHub Actions | |
| Quality tooling | ruff, mypy (light), pytest, sqlfluff (optional) | |

**Laptop budget:** about 10–12 GB RAM for everything. Stop the streaming job when you aren't working on it. Use `make up-core` (no Grafana, no stream) for dbt work.

---

## 4. Data sources

| Source | Endpoint | Format | Cadence | Limits and gotchas |
|---|---|---|---|---|
| Yahoo Finance via `yfinance` (EOD) | `yf.Ticker(t).history(start, end, interval="1d", auto_adjust=False, actions=True)` | DataFrame → staged as **JSON records** | Daily after US close (~22:00 UTC) | No key. Unofficial Yahoo API: throttles with `YFRateLimitError`, so retry with backoff; one request per ticker per run. OHLC are split-adjusted, `Adj Close` adds dividends, `Stock Splits` is 0.0 on normal days. Personal-use terms. |
| Yahoo Finance via `yfinance` (delayed intraday) | `history(period, interval="1m", prepost=False)` polled every 60 s by `streaming/equity_producer.py` | **JSON events** on topic `equity_bars_1m` | Continuous during US market hours, released **≥ 10 min late** (`QUOTE_DELAY_MINUTES`) | Simulates a free-tier delayed feed (like TradingView without a real-time subscription). Yahoo keeps days of 1m history, so a restarted producer refills its own gap. |
| _(old row, replaced)_ | | | | | Hourly and daily request caps: check your account page. History goes back decades, so a 5-year backfill is fine. Personal-use terms. |
| SEC EDGAR | `https://data.sec.gov/api/xbrl/companyfacts/CIK{10-digit}.json`; ticker → CIK from `https://www.sec.gov/files/company_tickers.json` | **Nested JSON** (XBRL facts) | Weekly check; new data only after a filing | No key. **Must send a descriptive `User-Agent` with your email.** Keep to ≤ 10 requests/second. Concept names vary by company (e.g. `Revenues` vs `RevenueFromContractWithCustomerExcludingAssessedTax`), so map aliases. Amendments (`10-Q/A`) restate values. |
| Binance trades | `wss://stream.binance.com:9443/ws/{symbol}@trade` (or the market-data-only `data-stream.binance.vision`) | **JSON events** | Continuous | No key. Connections drop after 24 hours, so reconnect with backoff. If blocked in your region, use Coinbase Exchange's public `matches` channel. |

**Changed 2026-11-25:** Tiingo was replaced by yfinance (no key), and a delayed intraday equity stream was added. See DECISIONS.md.

**Universe:** 12 US large caps across sectors (e.g. AAPL, MSFT, NVDA, AMZN, GOOGL, META, JPM, V, XOM, JNJ, WMT, KO) plus 3 crypto pairs (BTCUSDT, ETHUSDT, SOLUSDT). Store the list in the dbt seed `seeds/universe.csv` (ticker, cik, name, sector, asset_class) and read it everywhere from there.

**History:** 5 years of daily prices. All EDGAR facts from 2019 on.

Formats covered: CSV, nested JSON, streaming JSON events, Parquet/Iceberg, Markdown and JSON documents, and vector embeddings.

---

## 5. Data model

### 5.1 Common metadata columns (every Bronze/Silver table)
`_run_id STRING` (Airflow run or stream batch id), `_source STRING`, `_ingested_at TIMESTAMP`, `_payload_hash STRING` (sha256 of the raw record).

### 5.2 Bronze (Iceberg, namespace `bronze`)

| Table | Grain | Key columns | Partitioning |
|---|---|---|---|
| `prices_eod_raw` | 1 row per ticker × date per fetch | ticker, date, open, high, low, close, volume, adj_close, adj_volume, div_cash, split_factor, `_raw_line` (original CSV line) | `days(_ingested_at)` |
| `sec_companyfacts_raw` | 1 row per company per fetch | cik, ticker, `_payload` (full JSON string), payload_bytes | `days(_ingested_at)` |
| `crypto_trades` | 1 row per trade event | symbol, trade_id, price, qty, trade_time, event_time, is_buyer_maker, kafka_partition, kafka_offset | `days(trade_time)`, `bucket(4, symbol)` |
| `quarantine` | 1 row per rejected record | source, entity, reason, `_payload` | `days(_ingested_at)` |

**Idempotency rule (batch):** each run writes with a dynamic **partition overwrite** keyed on `(_source, logical date)`, so rerunning a date replaces it instead of appending. Streaming relies on Spark checkpoints, and Silver dedups by `(symbol, trade_id)` in case of replays.

### 5.3 Silver (Iceberg, namespace `silver`)

| Table | Grain / unique key | Built by |
|---|---|---|
| `prices_eod` | (ticker, trade_date) | Spark `MERGE INTO` from Bronze, latest `_ingested_at` wins |
| `financial_facts` | (cik, concept, unit, period_start, period_end, accession_no) | Spark: parse payload → `explode` facts → type → `MERGE INTO`. Keep all filings; Gold picks "as of". |
| `crypto_trades` | (symbol, trade_id) | Spark batch dedup every 15 minutes |
| `crypto_bars_1m` | (symbol, minute) | Streaming windowed aggregation (1-minute tumbling window, 2-minute watermark) |

All Silver tables have `_updated_at` for incremental downstream copies.

### 5.4 Warehouse (PostgreSQL `warehouse` DB)

| Schema | Purpose | Who can read |
|---|---|---|
| `lake_silver` | Copies of Silver tables (upsert by key, incremental on `_updated_at`) | `etl_writer`, `dbt` |
| `staging`, `intermediate`, `marts` | dbt layers | `dbt`; `marts` also to `grafana_ro`, `agent_ro` |
| `ops` | Pipeline telemetry | `etl_writer` writes; `grafana_ro`, `agent_ro` read |
| `agent` | `documents`, `chunks` (with `embedding vector(384)` and a `tsvector` column) | `agent_rw` |
| `pii` (stretch) | Synthetic subscriber contact details | `pii_admin` only |

### 5.5 `ops` schema

```
ops.pipeline_run    (run_id PK, dag_id, task_id, source, entity, logical_date, started_at, finished_at,
                     status, rows_read, rows_written, rows_quarantined, error_class, error_message_scrubbed)
ops.watermark       (source, entity, watermark_value, updated_at, PK(source, entity))
ops.reconciliation  (run_id, source, entity, logical_date, layer_from, layer_to, count_from, count_to,
                     missing, duplicates, late, status ['ok','mismatch'], checked_at)
ops.stream_metrics  (batch_id, window_start, topic, events, late_events, parse_errors,
                     max_event_lag_seconds, written_at)
ops.incident        (incident_id PK, detected_at, alert_name, severity, triage_note_path,
                     agent_diagnosis, human_verdict, resolved_at)
```

Error messages are **scrubbed** before storing: mask quoted literals, tokens and long digit runs. The triage agent reads this table, so it must never contain secrets.

---

## 6. Pipelines

### 6.1 Airflow DAGs

| DAG | Schedule (UTC) | Tasks |
|---|---|---|
| `prices_eod_daily` | `0 23 * * 1-5` | `extract_prices` (yfinance, per ticker, dynamic task mapping) → `validate_and_land_bronze` → `reconcile_source_bronze` → `spark_bronze_to_silver_prices` → `reconcile_bronze_silver` → `copy_silver_to_pg` → emits Asset `silver_prices` |
| `sec_facts_weekly` | `0 2 * * 6` | `extract_edgar` (per CIK, skip if `submissions` shows no new filing since watermark) → land Bronze → reconcile → `spark_bronze_to_silver_facts` → reconcile → copy → Asset `silver_facts` |
| `stream_silver_15m` | every 15 min | `spark_dedup_trades` → `copy_bars_to_pg` → `check_stream_health` (crypto; fails if no events in last 5 min); `copy_equity_bars_to_pg` |
| `dbt_build` | Asset-triggered by `silver_prices` / `silver_facts`, plus hourly for crypto | `dbt source freshness` → `dbt build` → `publish_dbt_results` (parse `run_results.json` into `ops`) |
| `fact_sheets` | after `dbt_build` | `render_fact_sheets` → `embed_changed_chunks` |
| `iceberg_maintenance` | daily 03:00 | `rewrite_data_files` on streaming tables, `expire_snapshots` (keep 7 days), `remove_orphan_files` |
| `backfill_prices` | manual | Same tasks as daily, over a date range in monthly chunks |

**Every task** goes through the `ops` instrumentation decorator (`@instrumented`), which writes `ops.pipeline_run` and is **fail-open** (telemetry errors never fail the ETL). Retries: 3 with exponential backoff. `on_failure_callback` sends a notification and creates an `ops.incident` row.

### 6.2 Streaming
- **Producer** (`streaming/producer.py`, own container): WebSocket → Redpanda topic `crypto_trades` (6 partitions, key = symbol), reconnect with backoff, `acks=all`, idempotent producer.
- **Delayed equity producer** (`streaming/equity_producer.py`, own container): polls yfinance 1-minute bars for the 12 equities every 60 s and publishes a bar to topic `equity_bars_1m` (6 partitions, key = ticker) only once it is complete **and** at least `QUOTE_DELAY_MINUTES` (default 10) old, like a free-tier feed. On start it re-reads `STARTUP_PERIOD` (5 d) to refill any gap. Re-published bars are absorbed downstream.
- **Spark job** (`spark_jobs/stream_market.py`, own container, `restart: unless-stopped`):
  - `readStream` from Kafka with `startingOffsets=earliest` the first time, then from the checkpoint.
  - Parse with an explicit schema. Bad records go to quarantine via `foreachBatch`.
  - Query A: raw trades → `bronze.crypto_trades` (append, trigger 30 s).
  - Query B: `withWatermark("trade_time", "2 minutes")` + 1-minute tumbling window → `silver.crypto_bars_1m` (append mode).
  - Query C: delayed equity bars → `bronze.equity_bars_1m` (append) + `MERGE` into `silver.equity_bars_1m` on `(ticker, minute)`, in `foreachBatch`.
  - `foreachBatch` metrics → `ops.stream_metrics`. Count late events as records whose `trade_time` is older than the current watermark.
  - Checkpoints live under `s3://lake/checkpoints/<query>`.
- **Restart test:** kill the container mid-stream, restart, and verify trade-count continuity by minute with no duplicate `trade_id`s.

### 6.3 Data quality and reconciliation (where each check lives)

| Check | Layer | Tool | On failure |
|---|---|---|---|
| Schema / types / required fields | Ingestion | pydantic models | Row → `bronze.quarantine`; run fails if quarantine rate > 5 % |
| Source count = Bronze count | Bronze | `ops.reconciliation` | Status `mismatch` → alert |
| Bronze distinct keys = Silver rows for the partition; duplicates counted | Silver | Spark + `ops.reconciliation` | Alert |
| Late records (streaming) | Silver | `ops.stream_metrics` | Alert if late rate > 1 % |
| Freshness | Warehouse | `dbt source freshness` (warn 26 h, error 50 h for prices) | `dbt_build` fails → incident |
| Keys, nulls, ranges, relationships | Gold | dbt tests | Build fails → incident |
| No look-ahead (`filed_date <= trade_date`) | Gold | Singular dbt test | Build fails |
| Missing trading days (gap > 4 calendar days) | Gold | Singular dbt test (warn) | Warn |

---

## 7. Spark performance exercise (`docs/PERFORMANCE.md`)

The real data is small, so make it big enough to show something: generate a scaled copy of `silver.prices_eod` and `financial_facts` (×50–100 with synthetic tickers) into a `perf` namespace.

**Protocol**
1. Baseline: run `bronze_to_silver_facts` (explode + dedup + join to the universe + `MERGE`) with defaults. Record wall time, number of stages, shuffle read/write, spill, and the number and size of output files (Spark UI, plus Iceberg `files` metadata table).
2. Find the actual bottleneck in the UI (skewed task, too many small files, a needless shuffle, a sort-merge join against a tiny table).
3. Apply **one change at a time**, re-measure, and keep a table:
   - `spark.sql.shuffle.partitions` sized to the data (or AQE coalescing)
   - Broadcast the small universe table
   - Partition pruning / predicate on the run's partition instead of a full scan
   - Avoid a Python UDF in the explode path (use built-in JSON functions)
   - Compact small files before reading; set the table's `write.target-file-size-bytes`
   - Skew: salting or AQE skew-join handling, if a synthetic "mega-ticker" causes skew
4. Write up: before/after numbers, *why* each change helped, and what you would do on a cluster.

---

## 8. dbt project (`dbt/`)

```
models/
  staging/       stg_prices_eod, stg_financial_facts, stg_crypto_bars_1m, stg_universe (seed)
  intermediate/  int_daily_returns            -- log returns with lag(), split-adjusted
                 int_fundamentals_quarterly   -- pivot concepts with alias mapping, latest filing per period
                 int_intraday_returns_1m      -- crypto + delayed equity 1m returns (equity resets each NY session)
  marts/         dim_company
                 fct_rolling_volatility       -- 20d / 60d stdev × sqrt(252), z-score vs trailing 1y
                 fct_drawdown                 -- running max, drawdown, max drawdown
                 fct_correlation_60d          -- pairwise rolling correlation
                 fct_fundamentals_asof        -- each trading day joined to the latest filing with filed_date <= trade_date
                 fct_intraday_volatility      -- crypto 60-minute rolling vol from 1m bars
                 mart_fact_sheet_inputs       -- one row per ticker × fiscal quarter, feeds the documents
seeds/           universe.csv, concept_aliases.csv
tests/           assert_no_lookahead.sql, assert_no_trading_gaps.sql, assert_returns_sane.sql
macros/          annualize_vol.sql
```

- **Incremental models with a lookback:** `fct_rolling_volatility` is `incremental` (`unique_key=['ticker','trade_date']`, `merge` strategy). On incremental runs it reprocesses the last 90 days, because a 60-day window needs earlier rows. Know how to explain this.
- **Tests:** `dbt_utils.unique_combination_of_columns` on every fact, `not_null`, `accepted_values` for sector and fiscal period, `relationships` to `dim_company`, `dbt_utils.accepted_range` for prices > 0 and volatility ≥ 0.
- **Docs:** describe every mart and the key columns, and agree on metric definitions in `docs/METRICS.md`, e.g. "annualized volatility = stdev of daily log returns over N trading days × √252".

---

## 9. Dashboard and alerts (Grafana, provisioned as code)

**Panels:** volatility by ticker (20d and 60d); volatility-regime heatmap (ticker × week); drawdown; correlation matrix; crypto intraday volatility; pipeline freshness per source; reconciliation status; stream events/min and lag; latest dbt test results.

| Alert | Rule | Severity |
|---|---|---|
| Volatility spike | 20d vol z-score > 3 for any ticker | info (business alert) |
| Missed daily load | No successful `prices_eod_daily` run since 30 h | high |
| Stream stalled | `ops.stream_metrics` has no rows for 5 minutes | high |
| Reconciliation mismatch | Any `ops.reconciliation.status = 'mismatch'` in the last 24 h | high |
| dbt failure | Latest `dbt_build` has failed tests or models | high |
| Quarantine spike | Quarantine rate > 5 % for a run | medium |

Route alerts to a webhook (Slack, Teams or Discord, whichever you have). Each alert links to its runbook.

---

## 10. AI layer

### 10.1 Agent-ready fact sheets
- One Markdown file per ticker × fiscal quarter in `data/fact_sheets/{ticker}/{fy}-{fp}.md`, rendered with Jinja from `mart_fact_sheet_inputs`.
  - Sections: company, period, filing date and accession number, key financials and QoQ/YoY change, margin and leverage ratios, volatility and drawdown over the quarter, data-quality notes (e.g. restated values).
- A JSON sidecar with metadata: ticker, cik, fy, fp, filed_date, source URLs, `content_hash`, generated_at.
- **Incremental embedding:** re-chunk and re-embed only when `content_hash` changes. Store in `agent.documents` / `agent.chunks` (`embedding vector(384)`, HNSW index; `tsv tsvector` with a GIN index).

### 10.2 Financial QA agent (reuse your existing LangGraph/Flask service)
- Retrieval is hybrid: pgvector cosine top-k plus Postgres full-text top-k, merged with **reciprocal rank fusion**. This replaces FAISS/BM25 files with one database.
- Answers must cite fact-sheet IDs.
- **Optional tool: natural-language SQL** on `marts` only, through the read-only role `agent_ro`, with `SELECT`-only checks, a table allowlist, a row limit and a statement timeout. This mirrors MoMo's "business teams query data in natural language".
- Evaluation: `agents/eval/qa_questions.yaml` (15–20 questions with expected document IDs) → Precision@5, Recall@5, plus answer spot checks.

### 10.3 Incident-triage agent
- **Trigger:** an `ops.incident` row (from an Airflow failure callback or a Grafana webhook), or the CLI `make triage INCIDENT=<id>`.
- **Tools, all read-only:**
  - `get_failed_runs(since)` and `get_reconciliation_issues(since)`, from `ops`
  - `get_stream_metrics(minutes)`
  - `get_dbt_failures()`, which parses `target/run_results.json`
  - `get_task_log(dag_id, run_id, task_id)`, via the Airflow 3 REST API (`/api/v2`), tail only, with secrets scrubbed
  - `search_runbooks(query)`, which does keyword + embedding search over `runbooks/*.md`
- **Output:** a Markdown triage note in `incidents/{incident_id}.md` with these sections: Summary · Evidence (with the queries and log lines used) · Likely cause · Confidence · Suggested next step (runbook section). It also sets `ops.incident.agent_diagnosis`.
- **Guardrails:** it suggests only and never runs fixes. It has no write access except its note. Logs are scrubbed before they reach the LLM, and the prompt and tool calls are logged.
- **Evaluation:** `scripts/inject_fault.py --fault <name>` with 4 faults, each with an expected diagnosis:

| Fault | How it's injected | Expected diagnosis |
|---|---|---|
| `api_500` | Yahoo is unreachable or returns 500 (e.g. a failing proxy in front of yfinance) | Upstream API outage → retry later (runbook: API failure) |
| `schema_change` | Rename a CSV column in a fixture response | Source schema drift → quarantine spike → update parser and contract |
| `duplicate_batch` | Land the same Bronze partition twice, bypassing overwrite | Duplicate load → reconciliation duplicates → rerun with overwrite |
| `stream_stop` | Stop the producer container | Stream stalled at the source, not in Spark → restart the producer |

  Report: correct diagnoses out of 4, plus time to diagnosis.

### 10.4 PII separation (stretch, about half a day)
- Synthetic `pii.subscribers` (name, email, phone) who subscribe to volatility alerts for tickers.
- Analytics tables use only `subscriber_key = HMAC-SHA256(secret, subscriber_id)`. Only the notifier service, using role `pii_admin`, joins back to contact details.
- `grafana_ro` and `agent_ro` cannot read `pii` (prove it with a test that expects a permission error).

---

## 11. Repository layout

```
.
├── CLAUDE.md
├── Makefile
├── .env.example                # never commit .env
├── docker/                     # Dockerfiles: airflow (JDK17 + pyspark + baked JARs), spark-stream, producer
├── infra/
│   ├── docker-compose.yml
│   ├── grafana/provisioning/   # datasources, dashboards (JSON generated by script), alerting
│   └── postgres/init/          # databases, schemas, roles, grants
├── src/lakehouse/
│   ├── config.py               # pydantic-settings, reads env
│   ├── ops/                    # @instrumented decorator, reconciliation helpers, scrubber
│   ├── ingestion/              # yahoo.py, edgar.py, contracts.py (pydantic), bronze_writer.py
│   ├── iceberg/                # catalog.py (PyIceberg), table definitions
│   └── docs_gen/               # fact sheet rendering + embedding
├── streaming/                  # producer.py (Binance), equity_producer.py (delayed yfinance bars)
├── spark_jobs/                 # stream_market.py, bronze_to_silver_*.py, copy_silver_to_pg.py, maintenance.py, common.py
├── dags/
├── dbt/
├── agents/                     # qa/, triage/, eval/
├── runbooks/                   # api_failure.md, stream_stalled.md, reconciliation_mismatch.md, dbt_failure.md
├── scripts/                    # init_lake.py, inject_fault.py, scale_perf_data.py, build_dashboard.py
├── tests/                      # unit/, integration/, fixtures/ (recorded API samples)
└── docs/                       # PLAN.md, PROGRESS.md, DECISIONS.md, VERSIONS.md, PERFORMANCE.md, METRICS.md
```

**Makefile targets:** `up`, `up-core`, `down`, `init` (buckets, catalog namespaces, schemas, roles), `backfill`, `stream-start`, `stream-stop`, `dbt-build`, `test`, `test-int`, `lint`, `fault FAULT=<name>`, `triage INCIDENT=<id>`, `eval`.

---

## 12. Testing and CI

| Level | What | Where |
|---|---|---|
| Unit | Parsers and contracts against **recorded** API samples, scrubber, watermark logic, fact-sheet rendering, RRF merge | `tests/unit`, no network, no Docker |
| Spark unit | Transform functions on small DataFrames (local SparkSession fixture) | `tests/unit/spark` |
| Integration | `make up-core` → land fixture data → run Spark job → assert Iceberg row counts and reconciliation rows | `tests/integration` (local only) |
| dbt | `dbt build` on seed-sized data (`--target ci`, Postgres service container) | CI |
| Agent eval | QA metrics and triage fault results | `make eval`, local, results committed to `docs/` |

GitHub Actions: `ruff` + `pytest tests/unit` + `dbt build --target ci` on each push. Never call real APIs in CI.

---

## 13. Day-by-day schedule

Each day ends with a commit, an updated `docs/PROGRESS.md`, and (when relevant) notes in `docs/DECISIONS.md`.

| Day | Deliverables | Acceptance criteria |
|---|---|---|
| **1 · Wed 25 Nov: Foundation** | Repo layout, Compose (SeaweedFS, REST catalog with persisted SQLite, Redpanda, Postgres + pgvector, Airflow standalone, Grafana); custom Airflow image with JDK 17 + pyspark + baked JARs; `make init`; `ops` schema and roles; `VERSIONS.md`; recorded API samples in `tests/fixtures` | PyIceberg creates a table → Spark appends → PyIceberg reads it back; the catalog survives `docker compose down && up`; one real call per source succeeds |
| **2 · Thu 26 Nov: Batch to Bronze** | yfinance and EDGAR extractors (retry, backoff, rate limit, User-Agent); pydantic contracts; quarantine; watermarks; `@instrumented`; `prices_eod_daily` and `sec_facts_weekly` DAGs up to Bronze + source reconciliation; 5-year backfill | Running the same logical date twice leaves Bronze counts unchanged; a malformed fixture row lands in quarantine; `ops.pipeline_run` and `ops.reconciliation` rows exist |
| **3 · Fri 27 Nov: Streaming** | Producer → Redpanda; Spark streaming queries A (Bronze trades) and B (1m bars with watermark); delayed yfinance equity feed (query C); `ops.stream_metrics`; `stream_silver_15m` DAG; compaction job | Kill and restart the stream: no gaps and no duplicate `trade_id`s by minute; late events counted |
| **4 · Sat 28 Nov: Silver + Spark tuning** | `bronze_to_silver_prices` and `_facts` with `MERGE INTO`; Bronze → Silver reconciliation; `copy_silver_to_pg`; scaled perf dataset; `PERFORMANCE.md` | Silver populated for all sources; each run has reconciliation rows; PERFORMANCE.md shows baseline vs tuned with at least 3 measured changes |
| **5 · Sun 29 Nov: dbt + Grafana** | Full dbt project with tests, freshness, docs and `METRICS.md`; `dbt_build` DAG triggered by Assets; dbt results into `ops`; Grafana dashboard + 6 alert rules provisioned | One end-to-end run goes from API to marts; `dbt build` green; at least one alert shown firing (screenshot) |
| **6 · Mon 30 Nov: AI agents** | Fact sheets + incremental embedding; QA agent on pgvector hybrid search + eval; triage agent with 5 tools; 4 runbooks; `inject_fault.py`; (stretch) NL-SQL tool and PII separation | Triage notes correct for ≥ 3 of 4 faults; QA agent answers with citations; metrics recorded |
| **7 · Tue 1 Dec: Hardening and portfolio** | Unit/Spark/integration tests; CI green; README (diagram, quick start, design decisions, screenshots/GIF, results); final CV numbers | Fresh clone → `make up && make init && make backfill` works in under 10 minutes (excluding image build); CI green |

**Cut order if behind:** correlation mart → extra Grafana panels → QA eval set → intraday volatility mart → NL-SQL and PII stretch.
**Never cut:** Iceberg + REST catalog, Kafka streaming with restart test, Spark tuning write-up, reconciliation, dbt tests, triage agent.

---

## 14. Progress checklist (copy into `docs/PROGRESS.md`)

```
## Day 1 Foundation
- [ ] Compose stack up; all services healthy
- [ ] Iceberg round trip PyIceberg <-> Spark; catalog persists across restart
- [ ] Postgres: databases, schemas, roles, ops tables
- [ ] Recorded fixtures for yfinance, EDGAR, Binance
- [ ] VERSIONS.md pinned
## Day 2 Batch → Bronze
- [ ] Contracts + quarantine
- [ ] Watermarks + idempotent partition overwrite
- [ ] Two DAGs to Bronze with reconciliation
- [ ] Backfill done
## Day 3 Streaming
- [ ] Producer with reconnect
- [ ] Spark streaming A + B with checkpoints
- [ ] Stream metrics + late events
- [ ] Restart test passed
## Day 4 Silver + tuning
- [ ] MERGE INTO Silver for prices, facts, trades
- [ ] Bronze→Silver reconciliation
- [ ] Copy to Postgres
- [ ] PERFORMANCE.md with measurements
## Day 5 dbt + Grafana
- [ ] Staging / intermediate / marts + tests + freshness
- [ ] Asset-triggered dbt DAG; results into ops
- [ ] Dashboard + 6 alerts
## Day 6 Agents
- [ ] Fact sheets + incremental embeddings
- [ ] QA agent + eval metrics
- [ ] Triage agent + runbooks + 4 fault results
- [ ] (stretch) NL-SQL tool, PII separation
## Day 7 Hardening
- [ ] Tests + CI green
- [ ] README + screenshots
- [ ] CV numbers collected
```

---

## 15. Risks and mitigations

| Risk | Mitigation |
|---|---|
| Spark ↔ Iceberg ↔ Kafka JAR version mismatch | Pin everything in `VERSIONS.md` on day 1; bake the JARs into the image; keep the round-trip smoke test in `make test-int` |
| SeaweedFS S3 quirks with Iceberg S3FileIO | Path-style access, explicit endpoint and region; fall back to Garage if the smoke test fails after 1 hour |
| REST catalog loses state | SQLite file on a named volume; restart test on day 1; Lakekeeper as fallback |
| API limits or blocks | Cache raw responses in Bronze; backfill in monthly chunks; Coinbase as Binance fallback; recorded fixtures for all tests |
| Laptop RAM | `up-core` profile; stop the stream when not needed; Spark driver memory 2 GB |
| Data too small for tuning to matter | Scaled synthetic perf dataset (section 7) |
| XBRL concept inconsistency | `concept_aliases.csv` seed plus a dbt test that each company has revenue for ≥ 80 % of quarters |
| Agent hallucination | Answers must cite; triage notes must quote evidence; human verdict column in `ops.incident` |
| Running out of time | Follow the cut order; keep the "never cut" list |

---

## 16. Numbers to collect for the CV (fill in `docs/RESULTS.md`)

- Tickers / pairs, years of history, rows in Bronze and Silver, Iceberg snapshots and files
- Stream: events/min, end-to-end lag (event time → Bronze commit), late-event rate, restart recovery result
- Spark: runtime and shuffle bytes before/after, files before/after compaction
- dbt: number of models, tests and sources with freshness
- Alerts: number of rules, one real catch if it happens
- Triage agent: correct diagnoses out of 4, median time to note
- QA agent: Precision@5 / Recall@5

---

## 17. Interview prep alongside the build (about 30 minutes a day)

- **Day 1:** B-tree indexes (and when they aren't used), transactions and isolation levels
- **Day 2:** idempotency, watermarks, at-least-once vs exactly-once
- **Day 3:** event time vs processing time, Spark watermarks, checkpoints, Kafka partitions and consumer offsets
- **Day 4:** Spark jobs, stages and tasks, shuffles, skew, AQE; Iceberg snapshots, `MERGE INTO`, compaction, time travel
- **Day 5:** window functions (practice 3 problems); dbt incremental strategies; metric definitions
- **Day 6:** RAG evaluation, hybrid search, guardrails for agents
- **Day 7:** practice explaining the architecture in 2 minutes, and the STAR stories from your internship
