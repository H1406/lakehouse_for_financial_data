"""Daily Iceberg maintenance: compaction, snapshot expiry, orphan-file cleanup."""

from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag
from common import DEFAULT_ARGS, spark_submit


@dag(schedule="0 3 * * *", start_date=datetime(2026, 11, 1), catchup=False, default_args=DEFAULT_ARGS,
     max_active_runs=1, tags=["maintenance"])
def iceberg_maintenance():
    BashOperator(task_id="rewrite_expire_remove_orphans", bash_command=spark_submit("maintenance", ""))


iceberg_maintenance()
