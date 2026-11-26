# Lakehouse for market volatility monitoring and financial AI agents

A laptop-scale lakehouse. Batch (yfinance EOD prices, SEC EDGAR XBRL facts) and streaming (Binance trades in real time,
plus US equity 1-minute bars on a **≥ 10-minute delay**, like a free-tier feed) data land in **Apache Iceberg** on SeaweedFS S3 through a shared REST catalog. **Spark** builds Bronze → Silver,
and **dbt** on PostgreSQL builds the Gold marts (rolling volatility, drawdown, correlation, point-in-time fundamentals).

```
yfinance 1d ─┐                                  ┌─> bronze.prices_eod_raw ──┐
EDGAR JSON  ─┴─ Airflow (validate, quarantine) ─┴─> bronze.sec_companyfacts ─┼─ Spark MERGE ─> silver.* ─┐
Binance WS ───> producer ────────┐                                           │                          │
yfinance 1m ──> equity-producer ─┴─> Redpanda ─> Spark Structured Streaming ─┼─> bronze.crypto_trades ──┤
                (released >= 10 min late)        (3 queries, one app)        ├─> silver.crypto_bars_1m ─┤
                                                                             └─> silver.equity_bars_1m ─┤
                                     Postgres lake_silver.* (incremental upsert) <──────────────────────┘
                                        └─> dbt staging → intermediate → marts
ops schema: pipeline_run · watermark · reconciliation · stream_metrics · incident · dbt_result
```

**How every component works, with links to the code: [docs/COMPONENTS.md](docs/COMPONENTS.md)** · results: [docs/RESULTS.md](docs/RESULTS.md) · tuning: [docs/PERFORMANCE.md](docs/PERFORMANCE.md)

Full spec: [docs/PLAN.md](docs/PLAN.md) · status: [docs/PROGRESS.md](docs/PROGRESS.md) ·
why: [docs/DECISIONS.md](docs/DECISIONS.md) · pins: [docs/VERSIONS.md](docs/VERSIONS.md) ·
metrics: [docs/METRICS.md](docs/METRICS.md)

## Quick start

Needs Docker (≥ 8 GB for the VM), Python 3.12, and JDK 17 for local Spark unit tests.

```bash
make .env                  # then fill SEC_USER_AGENT ("name you@example.com"); yfinance needs no key
make build && make up      # SeaweedFS, iceberg-rest, Postgres+pgvector, Airflow (:8080), Redpanda, producer, stream
make init                  # bucket, catalog DB, schemas/roles/ops tables, Iceberg tables, dbt seeds
make smoke                 # PyIceberg -> Spark -> PyIceberg round trip
make backfill              # 5y prices (12 yfinance calls, ~30 s) + EDGAR facts; then the Asset-triggered dbt_build runs
python3.12 -m venv .venv && .venv/bin/pip install -e '.[dev]' && make test   # ruff + unit tests (no network)
```

`make browse` opens a read-only web GUI at http://localhost:8501 (localhost only, no auth) with Postgres tables,
Iceberg tables (schema, partition spec, snapshots, data files, first rows) and the files in `s3://lake` and `data/`.
Add `&limit=100` to a URL for more rows (max 500).

`make up-core` starts the stack without the stream (lighter). `make stream-stop` stops only the Spark job: the
producer keeps buffering into Redpanda, so the stream resumes from its checkpoint without a gap.

## The delayed equity feed

`streaming/equity_producer.py` polls yfinance 1-minute bars every 60 s and publishes a bar to `equity_bars_1m` only once
it is complete and at least `QUOTE_DELAY_MINUTES` old (default 10; set it in `.env` to simulate a slower feed).
Everything downstream is genuinely streaming. Because Yahoo keeps days of 1-minute history, a restarted producer
refills its own gap (measured: 0 missing minutes after a 3-minute outage). Spark MERGEs Silver on `(ticker, minute)`,
so re-published bars are harmless. The feed is silent outside US market hours (about 14:30–21:00 UTC in winter).

## Agents (DeepSeek)

Two agents share one loop (`agents/core/loop.py`): reason → call read-only tools → observe → answer. Guardrails
cap steps, tool calls, time and tokens, block repeated identical calls, truncate tool output, and force a final
answer when a limit trips. Every run is logged to `agent.run_log`.

| Agent | Command | Tools |
|---|---|---|
| Financial QA | `make ask Q="How did NVIDIA's revenue grow in fiscal 2027 Q2?"` | hybrid search over 370 fact sheets (pgvector + full-text, RRF); guarded NL-SQL on `marts` |
| Incident triage | `make triage INCIDENT=<id>` → `incidents/<id>.md` | failed runs, reconciliation, stream metrics, dbt results, Airflow task logs, runbooks |

Set `DEEPSEEK_API_KEY` in `.env`. `make fault FAULT=api_500|schema_change|duplicate_batch|stream_stop` injects a real
fault, and `make eval` scores retrieval (no LLM), QA answers and triage diagnoses.

## Local UIs (all bound to 127.0.0.1)

| URL | What |
|---|---|
| http://localhost:8080 | Airflow 3 |
| http://localhost:3000 | Grafana (admin / `GRAFANA_ADMIN_PASSWORD`): dashboard + 6 alert rules |
| http://localhost:8501 | data browser (Postgres, Iceberg tables and files) |
| http://localhost:8090 | agents service (`/ask`, `/triage`, `/fact-sheets/refresh`, `/grafana-webhook`) |

## Guarantees

- **Idempotent batch:** Bronze replaces exactly its `(_source, _logical_date)` slice in one Iceberg commit. Silver
  uses `MERGE INTO` on documented keys. Postgres uses `INSERT … ON CONFLICT`.
- **Validated at the edge:** pydantic contracts. Rejected rows go to `bronze.quarantine` with a reason, and a run
  fails above a 5 % quarantine rate.
- **Reconciled:** every layer hop writes `ops.reconciliation` (counts, missing, duplicates).
- **Stream restart-safe:** checkpoints on S3 plus Silver dedup on `(symbol, trade_id)`. Killing the Spark job
  produced 0 missing and 0 duplicate trade ids (Binance trade ids are sequential, so gaps are measurable:
  `scripts/check_stream_continuity.py`).
- **No look-ahead:** fundamentals join prices only where `filed_date <= trade_date` (enforced by a dbt test).
