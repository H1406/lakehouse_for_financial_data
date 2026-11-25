"""SEC EDGAR companyfacts: weekly. Skips CIKs with no new 10-K/10-Q since their watermark."""

from datetime import date, datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task
from common import DEFAULT_ARGS, SILVER_FACTS, spark_submit

from lakehouse import universe
from lakehouse.ingestion import tasks as ing
from lakehouse.warehouse_copy import copy_table

SOURCE = "sec_edgar"


@dag(schedule="0 2 * * 6", start_date=datetime(2026, 11, 1), catchup=False, default_args=DEFAULT_ARGS,
     max_active_runs=1, tags=["fundamentals", "batch"])
def sec_facts_weekly():
    @task
    def companies() -> list[dict]:
        # prior_ciks: a company that re-registered (e.g. XOM -> ExxonMobil Holdings in 2026) keeps its history
        # under the old CIK, so both are fetched; dbt groups fundamentals by ticker.
        return [{"cik": cik, "ticker": r["ticker"]} for r in universe.load("equity")
                for cik in [r["cik"], *filter(None, r["prior_ciks"].split(";"))]]

    @task(max_active_tis_per_dagrun=2)
    def extract_edgar(company: dict, ds=None) -> dict | None:
        return ing.extract_edgar(cik=company["cik"], ticker=company["ticker"],
                                 logical_date=date.fromisoformat(ds), source=SOURCE)

    @task
    def land_bronze(fetched: list, ds=None, run_id=None) -> list:
        fetched = [f for f in fetched if f]
        ing.land_edgar_bronze(fetched=fetched, logical_date=date.fromisoformat(ds), run_id=run_id, source=SOURCE)
        return fetched

    @task
    def reconcile_source_bronze(fetched: list, ds=None, run_id=None) -> None:
        ing.reconcile_edgar_source_bronze(fetched=fetched, logical_date=date.fromisoformat(ds), run_id=run_id,
                                          source=SOURCE)

    to_silver = BashOperator(
        task_id="spark_bronze_to_silver_facts",
        bash_command=spark_submit("bronze_to_silver_facts",
                                  f"--logical-date {{{{ ds }}}} --run-id '{{{{ run_id }}}}' --source {SOURCE}"),
    )

    @task(outlets=[SILVER_FACTS])
    def copy_silver_to_pg(ds=None, run_id=None) -> dict:
        return copy_table(table="financial_facts", run_id=run_id, logical_date=ds)

    landed = land_bronze(extract_edgar.expand(company=companies()))
    reconcile_source_bronze(landed) >> to_silver >> copy_silver_to_pg()


sec_facts_weekly()
