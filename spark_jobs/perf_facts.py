"""Benchmark one variant of the production facts job (bronze_to_silver_facts.run) on the `perf` namespace.

Each variant runs in a fresh spark-submit (clean JVM, its own session config) and starts from the same target
state: the Silver table is rolled back to its base snapshot first. Metrics come from the Spark REST API (stages
of the benchmark job group) and from the Iceberg snapshot the MERGE committed.

    spark-submit spark_jobs/perf_facts.py --variant v1 [--all-slices] [--aqe false] [--shuffle 200]
                                          [--merge-mode copy-on-write|merge-on-read] [--spread 0]
"""

import argparse
import json
import time
from datetime import date
from pathlib import Path

import requests

from spark_jobs.bronze_to_silver_facts import run
from spark_jobs.common import spark_session

TARGET = "lake.perf.financial_facts"
RELAND = date(2026, 1, 11)
OUT = Path("/opt/lakehouse/data/perf/results.jsonl")


def stage_metrics(spark, group: str) -> dict:
    ui = spark.sparkContext.uiWebUrl
    app = spark.sparkContext.applicationId
    jobs = requests.get(f"{ui}/api/v1/applications/{app}/jobs", timeout=30).json()
    stage_ids = sorted({s for j in jobs if j.get("jobGroup") == group for s in j["stageIds"]})
    tot = {"stages": 0, "tasks": 0, "shuffle_read_mb": 0.0, "shuffle_write_mb": 0.0, "spill_mb": 0.0,
           "executor_run_s": 0.0}
    worst = {"ratio": 0.0, "max_task_s": 0.0, "median_task_s": 0.0, "stage": None}
    for sid in stage_ids:
        for a in requests.get(f"{ui}/api/v1/applications/{app}/stages/{sid}", timeout=30).json():
            if a.get("status") != "COMPLETE":
                continue
            tot["stages"] += 1
            tot["tasks"] += a["numTasks"]
            tot["shuffle_read_mb"] += a["shuffleReadBytes"] / 1e6
            tot["shuffle_write_mb"] += a["shuffleWriteBytes"] / 1e6
            tot["spill_mb"] += (a["memoryBytesSpilled"] + a["diskBytesSpilled"]) / 1e6
            tot["executor_run_s"] += a["executorRunTime"] / 1e3
            if a["numTasks"] >= 2:
                q = requests.get(f"{ui}/api/v1/applications/{app}/stages/{sid}/{a['attemptId']}/taskSummary"
                                 f"?quantiles=0.5,1.0", timeout=30).json()["executorRunTime"]
                med, mx = q[0] / 1e3, q[1] / 1e3
                if mx > 1 and mx / max(med, 0.01) > worst["ratio"]:
                    worst = {"ratio": round(mx / max(med, 0.01), 1), "max_task_s": round(mx, 1),
                             "median_task_s": round(med, 2), "stage": sid}
    return {k: round(v, 1) for k, v in tot.items()} | {"skew": worst}


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--variant", required=True)
    p.add_argument("--all-slices", action="store_true")
    p.add_argument("--aqe", default="true")
    p.add_argument("--shuffle", default="16")
    p.add_argument("--merge-mode", default="copy-on-write")
    p.add_argument("--spread", type=int, default=0)
    a = p.parse_args()
    spark = spark_session(f"perf_{a.variant}", {"spark.sql.adaptive.enabled": a.aqe,
                                                "spark.sql.shuffle.partitions": a.shuffle})
    snaps = spark.sql(f"select snapshot_id from {TARGET}.snapshots order by committed_at").collect()
    base = snaps[0]["snapshot_id"]  # the pre-loaded state every variant starts from
    spark.sql(f"call lake.system.rollback_to_snapshot('{TARGET}', {base})")
    spark.sql(f"alter table {TARGET} set tblproperties ('write.merge.mode'='{a.merge_mode}', "
              f"'write.update.mode'='{a.merge_mode}', 'write.delete.mode'='{a.merge_mode}')")

    spark.sparkContext.setJobGroup("bench", a.variant)
    t0 = time.monotonic()
    out = run(spark, logical_date=RELAND, run_id=f"perf_{a.variant}", source="sec_edgar", namespace="perf",
              all_slices=a.all_slices, spread_partitions=a.spread)
    wall = time.monotonic() - t0
    metrics = stage_metrics(spark, "bench")

    summary = spark.sql(f"select summary from {TARGET}.snapshots order by committed_at desc limit 1").first()[0]
    t1 = time.monotonic()
    spark.sql(f"select count(*), sum(value) from {TARGET}").collect()  # read cost after the MERGE (MoR pays here)
    read_s = time.monotonic() - t1
    result = {
        "variant": a.variant, "config": {"all_slices": a.all_slices, "aqe": a.aqe, "shuffle_partitions": a.shuffle,
                                         "merge_mode": a.merge_mode, "spread_partitions": a.spread},
        "wall_s": round(wall, 1), **metrics, "rows_read": out.get("rows_read"), "rows_merged": out.get("rows_written"),
        "merge_commit": {k: summary.get(k) for k in ("added-data-files", "deleted-data-files", "added-delete-files",
                                                    "added-records", "deleted-records", "added-files-size",
                                                    "total-data-files", "total-delete-files")},
        "read_after_merge_s": round(read_s, 1),
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("a") as f:
        f.write(json.dumps(result) + "\n")
    print("RESULT " + json.dumps(result))


if __name__ == "__main__":
    main()
