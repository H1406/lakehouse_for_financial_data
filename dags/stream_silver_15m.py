"""Every 15 min: dedup streamed crypto trades into Silver, copy crypto and (delayed) equity 1m bars to Postgres,
check the crypto stream is alive.
The stream itself is a separate long-running service; Airflow only schedules batch work and monitors it."""

from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task
from common import DEFAULT_ARGS, SILVER_CRYPTO, spark_submit

from lakehouse.ops import connect
from lakehouse.warehouse_copy import copy_table


class StreamStalled(Exception):
    pass


@dag(schedule="*/15 * * * *", start_date=datetime(2026, 11, 1), catchup=False, default_args=DEFAULT_ARGS,
     max_active_runs=1, tags=["crypto", "stream"])
def stream_silver_15m():
    dedup = BashOperator(
        task_id="spark_dedup_trades",
        bash_command=spark_submit("silver_crypto_trades",
                                  "--until '{{ data_interval_end | ts }}' --run-id '{{ run_id }}'"),
    )

    @task(outlets=[SILVER_CRYPTO])
    def copy_bars_to_pg(ds=None, run_id=None) -> dict:
        return copy_table(table="crypto_bars_1m", run_id=run_id, logical_date=ds)

    @task
    def copy_equity_bars_to_pg(ds=None, run_id=None) -> dict:
        # Silver is written by the stream itself (MERGE in foreachBatch); this is only the Postgres copy.
        return copy_table(table="equity_bars_1m", run_id=run_id, logical_date=ds)

    @task(retries=0)
    def check_stream_health() -> int:
        """Crypto only: the delayed equity feed is legitimately silent outside US market hours."""
        with connect() as conn:
            events = conn.execute(
                "select coalesce(sum(events), 0) from ops.stream_metrics "
                "where query_name = 'bronze_trades' and written_at > now() - interval '5 minutes'"
            ).fetchone()[0]
        if events == 0:
            raise StreamStalled("no stream events in the last 5 minutes (see runbooks/stream_stalled.md)")
        return int(events)

    dedup >> copy_bars_to_pg() >> check_stream_health()
    copy_equity_bars_to_pg()


stream_silver_15m()
