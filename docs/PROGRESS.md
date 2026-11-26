# Progress

Last updated: 2026-11-26 (session 3). An item is ticked only when its PLAN.md §13 acceptance criterion was
demonstrated against the running stack. "Built" means the code exists, is unit-tested where possible, and is
waiting on something named in the note.

**Session 2 (2026-11-25): Tiingo → yfinance, plus a delayed (≥ 10 min) intraday equity stream.** The batch path ran end to end on real data.

## Day 1 Foundation
- [x] Compose stack up; all services healthy. Grafana is deferred to the dashboard stage.
- [x] Iceberg round trip PyIceberg <-> Spark (`make smoke`); catalog persists across `make down && make up`.
      Catalog moved from SQLite to Postgres (`iceberg_catalog` DB) after SQLITE_BUSY under concurrent commits (see DECISIONS).
- [x] Postgres: databases, schemas, roles, ops tables (`infra/postgres/init/sql/*.sql`, re-applied by `make init`, which is a verified no-op on a second run)
- [x] Recorded fixtures for yfinance (daily incl. the AAPL 2020 split, 1m), EDGAR (companyfacts, submissions, tickers), Binance
- [x] VERSIONS.md pinned
- [x] One real call per source succeeds (yfinance, EDGAR, Binance)
- [x] Universe CIKs checked against SEC's ticker map. XOM changed (new CIK 0002115436; old 0000034088 kept as `prior_ciks`).

## Day 2 Batch → Bronze
- [x] Contracts + quarantine (unit tests: bad values, high<low, column drift → everything quarantined)
- [x] Watermarks + idempotent partition overwrite. `prices_eod_daily` ran twice for 2026-11-24: Bronze slice 72 → 72, Silver 15,048 → 15,048,
      and Postgres `max(_updated_at)` unchanged. An EDGAR rerun fetched 0 CIKs (watermarks), and Bronze was untouched.
- [x] Two DAGs to Bronze with reconciliation (source→bronze, bronze→silver, silver→postgres all `ok`)
- [x] Backfill done. 5 years × 12 tickers = 15,048 rows in 33 s; EDGAR: 13 CIKs → 151,851 facts in 49 s.

## Day 3 Streaming
- [x] Producer with reconnect (exponential backoff + jitter; `acks=all`, idempotent, 6 partitions keyed by symbol)
- [x] Spark streaming A (Bronze trades + quarantine + metrics) + B (1m bars, 2 min watermark) with s3a checkpoints
- [x] Stream metrics + late events (`ops.stream_metrics`; late = trade_time < max seen − 2 min; 0 late observed so far)
- [x] Restart test passed. Spark container SIGKILLed for 60 s: Silver had 0 missing and 0 duplicate trade ids for all 3 pairs, and the Bronze replay count was unchanged.
      Note: stopping the **producer** loses trades at the source (the WebSocket can't be replayed). Two such gaps
      (09:29:50–09:31:10 and 09:44:45–09:44:54 UTC on 2026-11-25) came from stack restarts. `make stream-stop` now stops only Spark.
- [x] `stream_silver_15m` DAG (renamed from crypto_silver_15m) green end to end; compaction job ran (30 → 3 files)
- [x] Delayed equity feed (query C). Live from the 14:30 UTC open: 12 bars/minute, released about 661 s after bar start (10 min delay + bar + poll).
      Producer-stop test: stopped for 3 min mid-session. After restart it re-read 5 days, and Silver had 0 missing minutes and 0 duplicates.

## Day 4 Silver + tuning
- [x] MERGE INTO Silver for prices, facts, trades (+ equity bars in the stream)
- [x] Bronze→Silver reconciliation for every source
- [x] Copy to Postgres (incremental on `_updated_at`, upsert) for prices, facts, crypto bars, equity bars
- [x] PERFORMANCE.md with measurements: 6 variants × 3 passes on a ×10 dataset; ≥ 3 measured changes (pruning, AQE/shuffle sizing, merge-on-read, skew spreading)

## Day 5 dbt + Grafana
- [x] Staging / intermediate / marts + tests + freshness. `dbt build` 63/63 green on real data, 0 warnings.
      Bug found and fixed: incremental `fct_rolling_volatility` stayed empty after a first build on empty data (null max).
- [x] Asset-triggered dbt DAG; results into ops. Observed under the real scheduler: `prices_eod_daily` → Asset → `dbt_build`
      (`asset_triggered__…`) → Asset `marts` → `fact_sheets`.
- [x] Dashboard + 6 alerts (Grafana 13.2.7, provisioned as code). Volatility spike fired on real data (NVDA z = 8.2) →
      webhook → incident #2. Screenshot: not taken (no headless browser in this setup).
- Data note: NVDA vol_20d = 127 %, z = 8.2 on 2026-11-24 (Yahoo shows −14 % on 11-20 and +31 % on 11-23). It matches the source;
  worth checking before trusting it as a real event.

## Day 6 Agents
- [x] Fact sheets + incremental embeddings: 370 sheets + 5 runbooks → 2,603 chunks (pgvector HNSW + tsvector GIN);
      re-embeds only changed `content_hash` (0 on a no-op refresh; 94 after a data fix)
- [ ] QA agent + eval metrics. Retrieval metrics done (recall@5 0.819 hybrid vs 0.764 / 0.681; MRR 0.701).
      Agent answers with the live LLM: **pending a DeepSeek API key** (`make eval`).
- [ ] Triage agent + runbooks + 4 fault results. Agent, 8 read-only tools, 5 runbooks, `inject_fault.py` with all 4 faults
      done; each produced exactly one real incident (19, 20, 24, 25). Diagnoses: **pending a DeepSeek API key**.
- [x] (stretch) NL-SQL tool (sqlglot guard + `agent_ro` + read-only + timeout + limit; 16 guard tests) and PII separation
      (HMAC keys, `pii_admin`-only notifier, 4 roles proven denied)

## Day 7 Hardening
- [ ] Tests + CI green. Locally: ruff clean, 55 unit + 10 integration tests pass. CI on GitHub: runs once this branch is pushed.
- [ ] README + screenshots: README updated; screenshots not taken (no headless browser here)
- [x] CV numbers collected: docs/RESULTS.md

## Open items
- DeepSeek key → `make eval` (QA answers + triage on incidents 19, 20, 24, 25; bonus real incidents 6, 18).
- Merge-on-read for `silver.financial_facts` + rewriting position deletes in maintenance (PERFORMANCE.md).
- Fresh-clone timing test (`make build up init backfill` < 10 min): not run; it needs the running stack stopped.
