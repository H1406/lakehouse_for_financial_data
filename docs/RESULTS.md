# Results (numbers for the CV)

Measured on a laptop (Docker VM: 8 CPUs, 7.6 GiB) between 2026-11-25 and 2026-11-26. Raw outputs are in `docs/eval/`.

## Data
- 12 US large caps + 3 crypto pairs; 5 years of daily prices (15,048 rows, yfinance) and EDGAR XBRL since 2019:
  13 CIKs → 151,851 facts → 370 company-quarter fact sheets.
- Backfill: 5 y × 12 tickers in 33 s; EDGAR load in 49 s. Rerunning a logical date leaves every layer's counts unchanged.
- Every layer hop is reconciled in `ops.reconciliation` (source → Bronze → Silver → Postgres).

## Streaming
- Binance trades: about 3–5k events per 30 s micro-batch; checkpointed restart (SIGKILL, 60 s down): 0 missing and 0
  duplicate trade ids in Silver.
- Delayed equity feed (free-tier style, ≥ 10 min): 12 bars/minute, ~661 s from bar start to publish. Producer stopped
  3 min mid-session → 0 missing minutes after restart (it refills its own gap).
- Compaction during streaming: 30 → 3, 72 → 1 and 68 → 1 files, with no lost commits.

## Spark tuning (see PERFORMANCE.md)
- Partition pruning: −58 % wall time, −66 % CPU, −56 % shuffle.
- Merge-on-read: a 20-fact restatement rewrote 20 records instead of all 1.54M (copy-on-write); −26 % CPU.
- Sized shuffle / AQE: 1,679 → 142 tasks; 200 → 9–11 output files per MERGE.
- End to end: 51.8 s → 16.0 s (median wall), 262.6 → 67.2 executor CPU-seconds.

## dbt and alerting
- dbt: 16 models, 2 seeds, 4 sources (2 with freshness), 47 data tests (4 singular); `dbt build` green on real data.
- Grafana: 12 panels, 6 alert rules as code; the volatility rule fired on real data (NVDA 20d vol z-score 8.2) and
  opened an incident through the agents webhook.

## Agents
- Retrieval over 375 documents (370 fact sheets + 5 runbooks) / 2,603 chunks (pgvector HNSW + Postgres full-text, RRF), 18 labelled questions:
  recall@5 **0.819** hybrid vs 0.764 vector-only vs 0.681 text-only; MRR **0.701** vs 0.459 vs 0.525.
- Incremental embedding: a no-change refresh embeds 0 documents; a data fix re-embedded exactly the 94 changed sheets.
- QA answers and triage diagnoses with the live LLM: see `docs/eval/agent_eval.json` (pending a DeepSeek API key;
  filled in by `make eval`).
- Fault injection: 4 faults, each produced exactly one real incident (14–401 s after injection).

## Engineering
- 55 unit tests (no network, no Docker), 10 integration tests (security + idempotency), ruff clean.
- Real defects found and fixed by running the system: Bronze wiped by a 100 %-quarantined run, Yahoo provisional bars,
  Airflow 3 manual runs without a logical date, a false restatement flag on 3 companies, non-atomic incident dedupe,
  failed runs losing their counts, maintenance vs stream commit races.
