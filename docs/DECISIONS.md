# Decisions

Each entry: the decision, the alternatives, and why. "Laptop simplification" marks where production would differ.

## Storage and catalog

- **Batch Bronze is partitioned by `_logical_date`, not `days(_ingested_at)`.** The plan listed `days(_ingested_at)`, but a rerun on a later day would land in a different partition and could not overwrite the first run. Partitioning on the run's logical date makes "rerun = replace my partition" literal. Alternative: delete by a non-partition column (rewrites files, slower). Streaming Bronze keeps `days(trade_time), bucket(4, symbol)`.
- **Bronze writes are a delete + append in one Iceberg transaction** (PyIceberg), filtered on `(_source, _logical_date[, cik])`. This is one atomic commit, so readers never see a half-replaced slice. Alternative: Spark `INSERT OVERWRITE` with dynamic partition overwrite. That needs a JVM per extract task, for small files.
- **Backfill lands under `_source = yfinance_backfill`.** If it shared `_source = yfinance`, a daily rerun on the backfill's logical date would overwrite 5 years of history with 7 days. Silver merges both sources by `(ticker, trade_date)`, latest `_ingested_at` wins.
- **The EDGAR overwrite is scoped to the CIKs fetched in that run.** EDGAR skips CIKs with no new filing (watermark). A rerun therefore fetches nothing, and a whole-partition overwrite would delete the first run's data. Scoping the delete to the fetched CIKs makes that rerun a no-op.
- **Silver prices and facts are unpartitioned.** They hold about 15k and about 200k rows. Partitioning would create many tiny files for no pruning benefit. Revisit for the scaled perf dataset.
- **The REST catalog's JdbcCatalog stores its state in Postgres (`iceberg_catalog` DB), not SQLite.** With SQLite, concurrent commits (two streaming queries plus compaction or batch jobs) failed with `SQLITE_BUSY`. After adding `busy_timeout` + WAL they failed with `SQLITE_BUSY_SNAPSHOT`, which a timeout can't fix. The stream crashed with `CommitStateUnknownException`. Fix: the same `iceberg-rest-fixture` image plus the Postgres JDBC driver on its classpath (`docker/iceberg-rest.Dockerfile`). Alternative: Lakekeeper (the plan's fallback), which means one more service with its own warehouse/storage-profile setup. The fix also removes the SQLite volume, and persistence comes from the Postgres volume. Note: the fixture logs its JDBC properties, including the laptop password, at startup.
- **Stream checkpoints moved to `s3a://lake/checkpoints/v2`** when the catalog moved. The old offsets belonged to tables that no longer existed in the new catalog. On the new path the job replayed everything still retained in Redpanda (165k trades) into the new tables. The old `warehouse/` files and `checkpoints/` prefix in the bucket are now unreferenced. `remove_orphan_files` cleans the table directories after 3 days, and the old checkpoint prefix can be deleted by hand.
- **`make stream-stop` stops only the Spark job.** The WebSocket can't be replayed, so stopping the producer loses trades for good (measured: two gaps during stack restarts). With the producer running, Redpanda buffers and Spark resumes from its checkpoint.

## Ingestion

- **Prices come from yfinance, not Tiingo (2026-11-25).** No account or key; the same idempotent DAGs, contracts and reconciliation apply. Trade-offs: it is an unofficial, scraped API (it can break or throttle, and has personal-use terms), and it has no adjusted open/high/low/volume. Those four columns were removed with Iceberg **schema evolution** (`sync_schema`: metadata only, column ids never reused) and a one-time Postgres migration. yfinance's `Close` is already split-adjusted, so returns use `adj_close` (split + dividend). The daily staging format is JSON records with yfinance's own column names, so Bronze `_raw_line` is the record exactly as received.
- **The Airflow image drops exactly one pin from Airflow's constraints file: `certifi`.** yfinance → curl-cffi 0.17 needs certifi ≥ 2026.10.19, and the constraints pin 2026.7.22. certifi is only a CA bundle, so a newer one is safe. Alternative: a separate venv like dbt's, which doesn't work for code imported by Airflow tasks.
- **XOM has two CIKs (`prior_ciks` in the universe seed).** Exxon re-registered as ExxonMobil Holdings in 2026: the history is under 0000034088, new filings under 0002115436 (found by checking SEC's ticker map, not memory). Both are fetched, and dbt groups fundamentals by ticker, so XOM's history and new quarters form one series.

- **The daily prices window comes from the logical date (`[ds-7d, ds]`), not from the watermark.** Rerunning a date then fetches exactly the same range (idempotent). The 7-day overlap picks up vendor corrections. The watermark is still recorded, for freshness and monitoring.
- **The backfill is one request per ticker for the whole range, not monthly chunks.** Monthly chunks would be 60 × 12 = 720 calls against an unofficial API that throttles (`YFRateLimitError`). 12 calls finish in about 30 s. Bronze keeps the raw records, so nothing is lost.
- **Extract → land handoff goes through files on the `data/` volume.** Mapped extract tasks write one file per ticker. A single land task validates and writes once, which avoids 12 concurrent Iceberg commits racing on the same partition. Laptop simplification: production would stage on an object-store prefix.
- **A header mismatch quarantines every row.** Schema drift (a renamed column) must not be half-parsed. The quarantine-rate check (> 5 %) then fails the run. The rejected rows are written *before* the check, so they can be inspected.

## Bronze → Silver (Spark)

- **`MERGE … WHEN MATCHED AND s._ingested_at >= t._ingested_at AND s._payload_hash <> t._payload_hash`.** An unchanged re-land does not bump `_updated_at`, so the downstream incremental copy stays a no-op. An older backfill rerun can't overwrite newer data.
- **Facts merge key uses null-safe equality (`<=>`).** `period_start` is null for instant (balance-sheet) facts, and `=` would never match them, so every rerun would insert duplicates.
- **Facts keep the `us-gaap` taxonomy only.** The plan's key has no taxonomy column. `dei`/`ifrs` facts would need it in the key. Shortcut, noted in code.
- **Bronze → Silver reconciliation runs inside the Spark job**, not as a separate Airflow task. It reuses the cached DataFrame and saves a second 20–30 s JVM start.
- **The crypto Silver dedup reprocesses a trailing 6 h every 15 minutes.** Trades that land late after a stream outage still reach Silver. MERGE `WHEN NOT MATCHED` keeps the overlap duplicate-free.

## Streaming

- **Delayed equity feed (simulating a free-tier, TradingView-style delay).** `equity_producer.py` polls yfinance 1-minute bars every 60 s and publishes a bar only when `minute + 1 min + QUOTE_DELAY_MINUTES (10) <= now`. Everything downstream is real streaming: continuous Kafka events, event-time metrics, Spark micro-batches every 30 s. Only the source is embargoed. Alternative: a tick WebSocket with an artificial delay buffer. Polling was chosen because Yahoo keeps days of 1-minute history: on restart the producer re-reads 5 days and refills its own gap. The Binance WebSocket can't do that (we measured gaps when the producer stopped).
- **Equity bars go Bronze (append) → Silver (`MERGE` on `(ticker, minute)`) inside `foreachBatch`**, not as a windowed aggregation like crypto bars, because the source already sends bars. The Silver row hash covers bar values only, so a re-published bar (new `published_at`, same values) is not an update and doesn't bump `_updated_at`.
- **Equity late events** use the same rule (bar older than newest seen − 2 min). After a producer restart, the 5-day re-publish shows up as late events. That's expected and harmless (MERGE absorbs it), but it will trip a naive "late rate > 1 %" alert, so the alert must exclude `query_name = 'equity_bars'` or allow for restarts.
- **No freshness or stall alert on equity bars.** The feed is silent outside US market hours, and its event lag is ≥ 600 s by design.
- **One Spark app, three queries** (`stream_market.py`), not a second container: it saves about 1 GB of RAM on an 8 GB Docker VM. Each query has its own checkpoint, so they recover independently.

- **`spark.sql.caseSensitive=true` in the stream job.** Binance events contain fields that differ only by case (`t`/`T`, `e`/`E`, `m`/`M`). Spark resolves names case-insensitively by default and raises `AMBIGUOUS_REFERENCE_TO_FIELDS`.
- **Checkpoints on `s3a://lake/checkpoints/<query>`.** Iceberg's S3FileIO only covers table files, so checkpoints need Hadoop's s3a. hadoop-aws 3.4.1 uses the SDK classes from iceberg-aws-bundle, plus two small SDK jars (transfer manager, apache client), instead of the ~600 MB full SDK bundle.
- **Late-event watermark is tracked on the driver** in `foreachBatch`. It resets on restart, so the first batch after a restart reports 0 late events. Shortcut: a durable version would read the max `trade_time` from Bronze on start.
- **The producer forwards the raw `data` object unchanged.** All parsing and validation happen in Spark against an explicit schema, so the contract lives in one place.

## Warehouse and dbt

- **Incremental lookbacks use `coalesce(max(trade_date), '1900-01-01')`.** Found on real data: the model was first built when prices were empty. On every later incremental run `max()` was null, `>= null` matched nothing, and the mart stayed empty forever with all tests green. Lesson: tests on an empty table pass trivially.
- **Intraday volatility annualizes per market:** crypto × √525,600 (24/7), equities × √98,280 (390 min × 252 days). Equity return and volatility windows reset each New York session, so an overnight gap is never treated as a 1-minute return.

- **Silver → Postgres copy uses PyIceberg + `COPY` + `INSERT … ON CONFLICT`, not Spark JDBC.** The volumes are small, and this avoids a JVM start and a JDBC driver jar. It reads `_updated_at > watermark − 5 min`. The overlap guards against commit-order races, and the upsert makes it harmless.
- **Prices freshness: `error_after` is 74 h, not the plan's 50 h.** A normal weekend (Fri 23:00 → Mon 23:00 = 72 h) would otherwise raise an error, and an incident, every hour of every weekend. `warn_after` stays at 26 h.
- **No freshness check on `financial_facts`.** Rows change only when a company files (quarterly), so the age of `_updated_at` is not a health signal. EDGAR health is covered by the DAG run and by reconciliation.
- **`fct_rolling_volatility` has two lookbacks.** It rewrites the last 90 days (output) but reads 500 calendar days (input). The z-score needs 252 prior `vol_20d` values, each built from 20 returns, so the input window must reach that far back.
- **Fundamentals keep only each filing's own period** (latest `period_end` per accession). A 10-Q also carries prior-year comparatives under the filing's `fy`/`fp`. Durations are checked (80–100 days for a quarter, 350–380 for FY) to drop year-to-date values.
- **dbt runs in its own venv inside the Airflow image.** dbt-core's dependency pins conflict with Airflow's constraints file.
- **`generate_schema_name` is overridden** so models land in `staging` / `intermediate` / `marts`, which already exist with grants, rather than in `<target>_<schema>`.

## Orchestration

- **Laptop simplification:** `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS=True` (no login). Production uses a real auth manager.
- **`dbt_build` is scheduled by `AssetOrTimeSchedule`**: it runs when `silver_prices` or `silver_facts` is updated, or hourly (for crypto bars). No sensors are used.
