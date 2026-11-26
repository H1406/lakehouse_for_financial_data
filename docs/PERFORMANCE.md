# Spark performance exercise: the EDGAR facts job

The job tuned here is the production `bronze_to_silver_facts.run` (parse multi-MB XBRL JSON → 4 × `explode` →
dedup → `MERGE INTO` Silver → reconciliation semi-join). The benchmark harness imports the real job. It only adds a
target namespace and two switches (`all_slices`, `spread_partitions`).

- Harness: [spark_jobs/perf_facts.py](../spark_jobs/perf_facts.py), runner [scripts/run_perf.sh](../scripts/run_perf.sh),
  summary [scripts/perf_summary.py](../scripts/perf_summary.py) → raw numbers in [eval/perf_results.json](eval/perf_results.json)
- Data: [scripts/scale_perf_data.py](../scripts/scale_perf_data.py) (`make perf`)

## Dataset

| | |
|---|---|
| Bronze `perf.sec_companyfacts_raw` | the 13 real EDGAR payloads × **10** under synthetic CIKs, spread over 10 daily partitions, plus one **"mega" company** (JPM's payload with every fact repeated 10×: a single 78 MB row), plus partition `2026-01-11`: a **re-land** of day 10 (14 payloads) with restated values |
| Silver `perf.financial_facts` | pre-loaded with days 1–10: **1,540,299 facts in 8 files** |
| Workload | process the re-land partition: 369,741 exploded facts → 173,640 distinct keys MERGEd; **20 facts actually changed** |

×10 rather than the plan's ×50–100: at ×20, the parse stage ran out of the 3 GB driver heap on the 8 GB Docker VM. The
copies are made inside Spark (cross join); an earlier Python-side version was OOM-killed. Everything below is local
mode, `local[4]`, 3 GB driver, with the stream stopped and ~10 other containers running.

## Protocol

Each variant changes **one thing** against the previous one. Each runs in a fresh `spark-submit` (clean JVM) and
starts from the same target state: the harness calls `rollback_to_snapshot` to the pre-loaded snapshot first. Three
interleaved passes; medians reported (v0/v1 had 2 valid runs). Metrics come from the Spark REST API (all stages of the
benchmark job group) and from the Iceberg snapshot the MERGE committed.

Wall time on a busy laptop is noisy (v2 ranged 24.9–43.9 s across passes). The more robust signals are total
executor CPU-seconds, task counts, shuffle bytes, and files/records rewritten.

## Results

| Variant | Change vs previous | Wall (median) | Executor CPU s | Tasks | Shuffle read | Files written / removed | Records rewritten |
|---|---|---|---|---|---|---|---|
| v0 baseline | full reprocess of all Bronze, AQE off, 200 shuffle partitions, copy-on-write | 51.8 s | 262.6 | 1,679 | 1,047 MB | 200 / 8 | 1,540,299 |
| v1 | **partition pruning**: read only the run's slice | 21.5 s | 88.8 | 1,667 | 461 MB | 200 / 8 | 1,540,299 |
| v2 | **AQE on** (coalesce shuffle partitions) | 25.0 s | 110.5 | 1,243 | 461 MB | **9** / 8 | 1,540,299 |
| v3 | **shuffle partitions sized** to the data (200 → 16) | 24.2 s | 90.1 | **142** | 369 MB | 11 / 8 | 1,540,299 |
| v4 | **merge-on-read** target (`write.merge.mode`) | 22.9 s | 66.6 | 110 | 332 MB | **1 data + 8 delete files** | **20** |
| v5 | **spread payloads** before parsing + by `(cik, concept)` | **16.0 s** | 67.2 | 493 | 348 MB | 1 data + 8 delete files | 20 |

Reading the merged table afterwards (`count(*), sum(value)`) took 0.3–0.55 s in every variant. At this size, the
merge-on-read read penalty (applying delete files) doesn't show yet.

## What each change did, and why

1. **Partition pruning (v0 → v1): −58 % wall, −66 % CPU, −56 % shuffle.** The naive job re-parsed and re-merged all
   145 payloads (2.1M exploded facts) to apply 14. Filtering on `_logical_date` (Bronze's partition column) makes Iceberg
   plan only that partition's files. This is the production job's default: it always filters to the run's slice.
2. **AQE (v1 → v2): fewer tasks, 200 → 9 output files; no wall-time win here.** With AQE off, every shuffle uses
   200 partitions. That is 200 mostly empty tasks per stage, and the final write produces **200 small files per MERGE**.
   That's the small-files problem being created by the job itself. AQE's partition coalescing fixes the file count, but its
   extra planning stages (20 → 23) cost a little CPU at this data size. It's still worth it: 200 files per daily run
   would degrade every future read and need compaction.
3. **Sized shuffle partitions (v2 → v3): 1,243 → 142 tasks, −20 % shuffle, −18 % CPU.** Fewer, fuller partitions mean
   less per-task scheduling overhead. With AQE on, the remaining gain comes from the starting point: AQE can only merge
   partitions, never split them below the initial count.
4. **Merge-on-read (v3 → v4): 1.54M → 20 records rewritten, −26 % CPU.** This is the biggest *structural* finding.
   With copy-on-write, a MERGE rewrites every data file that contains a matched row. The re-land matched rows in all 8
   files, so the job **rewrote the entire 1.54M-row table to change 20 facts**. With merge-on-read, it writes the 20 new
   rows plus 8 small position-delete files. The cost moves to readers, which must apply the deletes, and to maintenance,
   because `rewrite_data_files` must periodically fold deletes back in. That is the right trade for a table that gets
   frequent small corrections and is read in bulk.
5. **Spreading the work (v4 → v5): −30 % wall, with the lowest variance of all variants (15.8–16.5 s).** The re-land
   has 14 payloads in very few Parquet files. Parsing (`from_json`, the expensive step) therefore ran in roughly as many
   tasks as there were input files. `repartition(64)` of the payload rows before parsing gives each payload its own
   task. A second `repartition(64, cik, concept)` after the first explode spreads one company's ~12k concepts.
   Honest limit: the mega row is still parsed by **one** task (max task ≈ 2 s in both v4 and v5), because a single
   78 MB JSON string can't be split before it is parsed. The win comes from parallelising the other 13 payloads around
   it. The fix for the mega row itself would be upstream: land EDGAR per taxonomy or per concept group instead of one
   payload per company.

**Applied to production:** partition pruning was already the default. AQE is on in `spark_jobs/common.py`, with
16 shuffle partitions by default. `spread_partitions` is available but off by default: real daily slices are ≤ 13
payloads, so the shuffle isn't worth it there. Merge-on-read for `silver.financial_facts` is the recommended next
change. It needs the maintenance DAG to also rewrite position deletes, and that follow-up is noted in DECISIONS.md.

## On a real cluster I would also

- **Size `spark.sql.shuffle.partitions` to bytes, not guesses:** about 128–200 MB per partition after AQE, using
  `spark.sql.adaptive.advisoryPartitionSizeInBytes`.
- **Bucket or sort the Silver target by `cik`** (`write.distribution-mode=hash`, a sort order on `cik, concept`), so a
  MERGE touches only the files for the CIKs in the slice. Then copy-on-write MERGEs stop rewriting the whole table even
  without merge-on-read.
- **Turn on storage-partitioned joins** (`spark.sql.sources.v2.bucketing.enabled`) so a MERGE between co-bucketed source
  and target avoids the shuffle entirely.
- **Watch skew in the Spark UI** (max vs median task time per stage) and use AQE skew-join handling
  (`spark.sql.adaptive.skewJoin.enabled`) for skewed joins. Row-level skew like the mega payload needs data-layout
  changes instead.
- **Run compaction and delete-file rewriting on a schedule** keyed to the number of delete files, not just the calendar.
