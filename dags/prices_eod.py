"""Yahoo Finance (yfinance) EOD prices: daily DAG and manual backfill. Source -> Bronze -> Silver -> Postgres."""

from datetime import date, datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Param, dag, task
from common import DEFAULT_ARGS, RUN_DATE, SILVER_PRICES, run_date, spark_submit

from lakehouse import universe
from lakehouse.ingestion import tasks as ing
from lakehouse.warehouse_copy import copy_table


def build(dag_id: str, source: str, schedule: str | None, params: dict) -> None:
    @dag(dag_id=dag_id, schedule=schedule, start_date=datetime(2026, 11, 1), catchup=False,
         default_args=DEFAULT_ARGS, max_active_runs=1, params=params, tags=["prices", "batch"])
    def prices():
        @task
        def tickers() -> list[str]:
            return [r["ticker"] for r in universe.load("equity")]

        @task(max_active_tis_per_dagrun=4)
        def extract_prices(ticker: str, dag_run=None, params=None) -> str:
            ld = run_date(dag_run)
            if params.get("start"):
                start, end = date.fromisoformat(params["start"]), date.fromisoformat(params["end"])
            else:
                start, end = ing.prices_window(ld)
            return ing.extract_prices(ticker=ticker, start=start, end=end, logical_date=ld, source=source)

        @task
        def validate_and_land_bronze(paths: list[str], dag_run=None, run_id=None) -> list[str]:
            ing.land_prices_bronze(paths=list(paths), logical_date=run_date(dag_run), run_id=run_id,
                                   source=source)
            return list(paths)

        @task
        def reconcile_source_bronze(paths: list[str], dag_run=None, run_id=None) -> None:
            ing.reconcile_prices_source_bronze(paths=paths, logical_date=run_date(dag_run), run_id=run_id,
                                               source=source)

        # Bronze -> Silver reconciliation runs inside the Spark job (saves a second JVM start).
        to_silver = BashOperator(
            task_id="spark_bronze_to_silver_prices",
            bash_command=spark_submit("bronze_to_silver_prices",
                                      "--logical-date " + RUN_DATE + " --run-id '{{ run_id }}' --source " + source),
        )

        @task(outlets=[SILVER_PRICES])
        def copy_silver_to_pg(dag_run=None, run_id=None) -> dict:
            return copy_table(table="prices_eod", run_id=run_id, logical_date=run_date(dag_run))

        landed = validate_and_land_bronze(extract_prices.expand(ticker=tickers()))
        reconcile_source_bronze(landed) >> to_silver >> copy_silver_to_pg()

    prices()


build("prices_eod_daily", "yfinance", "0 23 * * 1-5", {})
# One request per ticker for the whole range (12 calls). Lands under its own _source so a daily run on the
# same logical date can never overwrite the backfilled history.
build("backfill_prices", "yfinance_backfill", None,
      {"start": Param("2021-11-25", type="string", format="date"),
       "end": Param("2026-11-24", type="string", format="date")})
