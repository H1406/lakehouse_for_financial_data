# Progress

Last updated: 2026-11-26 (session 4: live-LLM evaluation). An item is ticked only when its PLAN.md §13 acceptance criterion was
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
- [x] QA agent + eval metrics. Retrieval: recall@5 0.819 hybrid vs 0.764 vector / 0.681 text, MRR 0.701.
      Live LLM (`deepseek-flash`, 18 questions): cites a relevant fact sheet 18/18, every citation grounded 18/18,
      expected numbers present 18/18, no guardrail stops; median 2.8 s and ~2.9k tokens per question.
      (First run: 17/18 by the metric; the "miss" was a correct answer cited from SQL instead of a fact sheet.)
- [x] Triage agent + runbooks + 4 fault results: **4/4 correct**, median 28 s to a note.
      First run was 3/4: schema change was misread as an API outage (no quarantine reasons visible), and stream stop
      got the right label for the wrong reason (the incident did not say which stream). Fixed by giving the agent
      the evidence: top quarantine reasons in the error, alert labels/summary in `ops.incident.details`
      (DECISIONS: "Triage evidence"). After the fix, those two took 13 s and 17 s (was 34 s and 51 s).
      Bonus real incidents (not scored): #6 provisional bar → data_freshness, #18 maintenance race → infrastructure,
      #21 Airflow 3 `ds` bug → infrastructure, #47 overnight freshness failure → infrastructure (stopped by the
      `blocked_calls` guardrail, still produced a usable note).
- [x] (stretch) NL-SQL tool (sqlglot guard + `agent_ro` + read-only + timeout + limit; 16 guard tests) and PII separation
      (HMAC keys, `pii_admin`-only notifier, 4 roles proven denied)

## Day 7 Hardening
- [x] Tests + CI green. Locally: ruff clean, 65 unit + 10 integration tests pass. GitHub CI green on `main` and on
      `stage-3-triage-evidence` (84f5416, run 45955412610).
- [ ] README + screenshots: README updated; screenshots not taken (no headless browser here)
- [x] CV numbers collected: docs/RESULTS.md

## Data browser v2 (2026-11-26)
- [x] Flow map (53 nodes, 72 hops; dbt lineage from `manifest.json`, 9 batch hops coloured by `ops.reconciliation`),
      storage map (treemaps + on-disk vs live bytes), file browser with Parquet footer/column-chunk view, Iceberg
      storage diagram + file-size histogram, agent knowledge base (coverage, chunks, embedding neighbours), agent runs
      with retrieved chunks. Every view exercised against the live stack with a DOM stub (no headless browser renders
      here); `pii` refused, `../` refused, legacy URLs redirect.
- [x] `agent.run_log` keeps structure after scrubbing (`scrub_tree`); tool events log structured output. Unit tests:
      `test_trace.py`, `test_lineage.py`, loop test. 65 unit tests pass.
- [ ] Restart the `agents` container so new runs use the fixed logging, then confirm one QA run's retrieved chunks
      show in `#/runs/<id>`. The 70 searches logged before the fix stay unreadable (labelled "before fix").
- [ ] Screenshot for the README: take it from a real browser.

## Open items
- **Overnight scheduler backlog (found 2026-11-26):** `stream_silver_15m` fell hours behind; 7 `spark_dedup_trades`
  runs are stuck in `running` with no failure row (likely killed, e.g. out of memory), so bar copies stalled and
  `dbt_source_freshness` failed ~hourly (11 incidents, 05:53–15:16 UTC). The triage agent reached the same reading
  (#47). Next: find why the dedup JVMs die (memory with the agents container + stream running), then cap
  concurrency (`max_active_runs=1` is set; also a task timeout) so a hung run cannot block the queue.
- `.env` sets `DEEPSEEK_MODEL=deepseek-flash-v4`, which DeepSeek rejects (HTTP 400): use `deepseek-flash` or
  `deepseek-v4-pro`. The eval ran with an override.
- After review: `make fault-cleanup FAULT=duplicate_batch` (Bronze slice 2026-11-24 still holds duplicates; Silver is
  unaffected), then resolve the open eval incidents.
- Note 64 claims `Stock Splits` was renamed too: false, caused by cutting the reason at 160 characters mid-name.
  Fixed (limit 400 + a test); the stored message for #64 keeps the old cut.
- Merge-on-read for `silver.financial_facts` + rewriting position deletes in maintenance (PERFORMANCE.md).
- Fresh-clone timing test (`make build up init backfill` < 10 min): not run; it needs the running stack stopped.
