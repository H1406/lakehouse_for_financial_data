"""dbt: source freshness -> build -> publish results to ops. Triggered by Silver assets, plus hourly
for crypto bars."""

from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task
from airflow.timetables.assets import AssetOrTimeSchedule
from airflow.timetables.trigger import CronTriggerTimetable
from common import DEFAULT_ARGS, MARTS, SILVER_FACTS, SILVER_PRICES

from lakehouse.dbt_results import publish_run_results

DBT = "cd /opt/lakehouse/dbt && dbt"


@dag(
    schedule=AssetOrTimeSchedule(timetable=CronTriggerTimetable("0 * * * *", timezone="UTC"),
                                 assets=(SILVER_PRICES | SILVER_FACTS)),
    start_date=datetime(2026, 11, 1), catchup=False, default_args=DEFAULT_ARGS, max_active_runs=1,
    tags=["dbt", "gold"],
)
def dbt_build():
    freshness = BashOperator(task_id="dbt_source_freshness", bash_command=f"{DBT} source freshness", retries=0)
    build = BashOperator(task_id="dbt_build", bash_command=f"{DBT} build", retries=1, outlets=[MARTS])

    @task(trigger_rule="all_done")  # publish results even when the build failed
    def publish_dbt_results() -> dict:
        return publish_run_results("/opt/lakehouse/dbt/target/run_results.json")

    freshness >> build >> publish_dbt_results()


dbt_build()
