"""Parse dbt's target/run_results.json into ops.dbt_result."""

import json
from pathlib import Path

from lakehouse.ops import connect, instrumented, scrub


@instrumented(source="dbt", entity="run_results")
def publish_run_results(path: str) -> dict:
    p = Path(path)
    if not p.exists():
        return {"rows_read": 0, "rows_written": 0}
    data = json.loads(p.read_text())
    meta = data["metadata"]
    rows = [
        (meta["invocation_id"], r["unique_id"], r["unique_id"].split(".")[0], r["status"], r.get("failures"),
         r.get("execution_time"), scrub(r.get("message")), meta["generated_at"])
        for r in data["results"]
    ]
    with connect() as conn:
        conn.cursor().executemany(
            """insert into ops.dbt_result (invocation_id, unique_id, resource_type, status, failures,
                   execution_time_s, message_scrubbed, generated_at)
               values (%s, %s, %s, %s, %s, %s, %s, %s)
               on conflict (invocation_id, unique_id) do nothing""",
            rows,
        )
    failed = [r for r in data["results"] if r["status"] in ("error", "fail")]
    return {"rows_read": len(rows), "rows_written": len(rows), "failed": len(failed)}
