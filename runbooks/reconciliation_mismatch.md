# Runbook: reconciliation mismatch

**Signals:** a row in `ops.reconciliation` with `status = 'mismatch'`.

## Diagnose
1. `select * from ops.reconciliation where status = 'mismatch' order by checked_at desc limit 20;`
2. Read the columns:
   - `duplicates > 0` at source→bronze or bronze→silver → the same slice was landed twice without overwrite (or a bug bypassed `overwrite_slice`).
   - `missing > 0` at source→bronze → rows parsed but not written: check the land task log and `bronze.quarantine` for that `_logical_date`.
   - `count_to < count_from` at bronze→silver → MERGE failed partway or keys collided; check the Spark task log.
3. Crypto: replayed Bronze rows after a stream restart are expected and are *not* a mismatch; a Silver mismatch is.

## Fix
- Duplicates in Bronze: rerun the DAG for that logical date (land overwrites the slice), then rerun the Spark step.
- Missing: fix the cause (parser, contract), then rerun from extract. All steps are idempotent.
