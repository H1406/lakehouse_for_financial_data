#!/usr/bin/env bash
# PERFORMANCE.md benchmark: each variant changes ONE thing vs the previous one. REPEATS interleaved passes
# (inside the airflow container: make perf). Results: data/perf/results.jsonl -> scripts/perf_summary.py
set -euo pipefail
cd /opt/lakehouse
REPEATS=${REPEATS:-3}
rm -f data/perf/results.jsonl
SUBMIT="spark-submit --master local[4] --driver-memory 3g spark_jobs/perf_facts.py"
run() { $SUBMIT --variant "$@" 2>/dev/null | grep -c '^RESULT ' >/dev/null || echo "variant $1 failed"; }
for pass in $(seq 1 "$REPEATS"); do
  echo "pass $pass"
  run v0_baseline        --all-slices --aqe false --shuffle 200 --merge-mode copy-on-write
  run v1_prune_slice                  --aqe false --shuffle 200 --merge-mode copy-on-write
  run v2_aqe                          --aqe true  --shuffle 200 --merge-mode copy-on-write
  run v3_shuffle_sized                --aqe true  --shuffle 16  --merge-mode copy-on-write
  run v4_merge_on_read                --aqe true  --shuffle 16  --merge-mode merge-on-read
  run v5_spread_skew                  --aqe true  --shuffle 16  --merge-mode merge-on-read --spread 64
done
python3 scripts/perf_summary.py
