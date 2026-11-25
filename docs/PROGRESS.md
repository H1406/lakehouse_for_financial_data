# Progress

Last updated: 2026-11-25 (session 1). An item is ticked only when its PLAN.md §13 acceptance criterion was
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
- [ ] PERFORMANCE.md with measurements. Out of scope for this stage.

## Day 5 dbt + Grafana
- [x] Staging / intermediate / marts + tests + freshness. `dbt build` 63/63 green on real data, 0 warnings.
      Bug found and fixed: incremental `fct_rolling_volatility` stayed empty after a first build on empty data (null max).
- [ ] Asset-triggered dbt DAG; results into ops. The DAG and results publishing work (an earlier freshness failure opened incident #1);
      the Asset trigger itself has not been observed yet (the runs so far used `dags test`).
- [ ] Dashboard + 6 alerts. Out of scope for this stage.
- Data note: NVDA vol_20d = 127 %, z = 8.2 on 2026-11-24 (Yahoo shows −14 % on 11-20 and +31 % on 11-23). It matches the source;
  worth checking before trusting it as a real event.

## Day 6 Agents
- [ ] Fact sheets + incremental embeddings
- [ ] QA agent + eval metrics
- [ ] Triage agent + runbooks + 4 fault results. **4 runbooks written** (also cited by DAG error messages).
- [ ] (stretch) NL-SQL tool, PII separation

## Day 7 Hardening
- [ ] Tests + CI green. Locally: ruff clean, 24 unit tests pass (0 skipped). The CI workflow has not run on GitHub yet.
- [ ] README + screenshots
- [ ] CV numbers collected
