"""Shared DAG settings. DAG files stay thin: task bodies live in src/lakehouse and spark_jobs/."""

from datetime import timedelta

from airflow.sdk import Asset

from lakehouse.ops import on_failure_callback

DEFAULT_ARGS = {
    "retries": 3,
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
