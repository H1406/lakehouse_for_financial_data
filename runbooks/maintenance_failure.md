# Runbook: Iceberg maintenance failure

**Signals:** `iceberg_maintenance.rewrite_expire_remove_orphans` failed; error `MaintenanceFailed: N of M maintenance calls failed: <procedure>(<table>): ...`.

## Diagnose
1. The error lists exactly which procedure failed on which table.
2. `CommitFailedException: ... branch main has changed` → an optimistic-concurrency conflict with the stream, which commits every 30 s to the same tables. Harmless: no data is lost, the other tables were still maintained.
3. Anything else (S3 errors, out of memory) → read the task log for the first `Caused by`.

## Fix
- Conflicts: just let the next daily run retry (or clear the task). Compaction already commits file groups separately (`partial-progress.enabled`) to shrink the race window. If conflicts persist, run maintenance with the stream stopped (`make stream-stop`; the producer keeps buffering).
- Never run compaction from the stream job itself.
