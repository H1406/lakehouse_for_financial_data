"""Median of each variant over the benchmark passes -> docs/eval/perf_results.json (+ a printed table)."""

import json
import statistics
from pathlib import Path

rows = [json.loads(line) for line in Path("/opt/lakehouse/data/perf/results.jsonl").read_text().splitlines()]
out = {}


def med(rs: list[dict], key: str) -> float:
    return statistics.median(r[key] for r in rs)


for v in dict.fromkeys(r["variant"] for r in rows):
    rs = [r for r in rows if r["variant"] == v]
    m = rs[0]["merge_commit"]
    out[v] = {"runs": len(rs), "config": rs[0]["config"], "wall_s_median": med(rs, "wall_s"),
              "wall_s_all": [r["wall_s"] for r in rs], "executor_run_s": med(rs, "executor_run_s"),
              "tasks": med(rs, "tasks"),
              "stages": med(rs, "stages"), "shuffle_read_mb": med(rs, "shuffle_read_mb"),
              "shuffle_write_mb": med(rs, "shuffle_write_mb"), "spill_mb": med(rs, "spill_mb"),
              "max_task_s": statistics.median(r["skew"]["max_task_s"] for r in rs),
              "rows_read": rs[0]["rows_read"], "rows_merged": rs[0]["rows_merged"],
              "files_added": m["added-data-files"], "files_removed": m["deleted-data-files"],
              "delete_files_added": m["added-delete-files"], "records_written": m["added-records"],
              "read_after_merge_s": med(rs, "read_after_merge_s")}
Path("/opt/lakehouse/docs/eval").mkdir(parents=True, exist_ok=True)
Path("/opt/lakehouse/docs/eval/perf_results.json").write_text(json.dumps(out, indent=1))
for v, r in out.items():
    print(f"{v:18} wall {r['wall_s_median']:5}s {r['wall_s_all']}  exec {r['executor_run_s']:6}s "
          f"tasks {r['tasks']:6} "
          f"shuffle {r['shuffle_read_mb']:7}MB max_task {r['max_task_s']}s "
          f"files +{r['files_added']}/-{r['files_removed']} "
          f"del+{r['delete_files_added']} records_written {r['records_written']}")
