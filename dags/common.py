"""Shared DAG settings. DAG files stay thin: task bodies live in src/lakehouse and spark_jobs/."""

import os
from datetime import date, timedelta

from airflow.sdk import Asset

from lakehouse.ops import on_failure_callback

DEFAULT_ARGS = {
    "retries": int(os.getenv("LAKEHOUSE_TASK_RETRIES", "3")),  # fault injection sets 0 to fail fast
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "on_failure_callback": on_failure_callback,
}

SILVER_PRICES = Asset("silver_prices")
SILVER_FACTS = Asset("silver_facts")
SILVER_CRYPTO = Asset("silver_crypto")
MARTS = Asset("marts")


def spark_submit(job: str, args: str) -> str:
    """Batch Spark jobs run with spark-submit inside the Airflow container (local mode)."""
    return f"spark-submit --master 'local[*]' --driver-memory 2g /opt/lakehouse/spark_jobs/{job}.py {args}"


# Airflow 3: runs created by `airflow dags trigger` / the UI have NO logical date (ds is None); scheduled runs do.
# Every DAG derives its business date here, so manual runs and backfills work the same way as scheduled ones.
RUN_DATE = "{{ (dag_run.logical_date or dag_run.run_after).strftime('%Y-%m-%d') }}"


def run_date(dag_run) -> date:
    return (dag_run.logical_date or dag_run.run_after).date()
