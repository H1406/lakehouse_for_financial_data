"""Fact sheets for the agents: after every dbt build (Asset `marts`), render fact sheets + runbooks and
re-embed only the documents whose content_hash changed. The work runs in the agents service, which holds the
embedding model; Airflow only orchestrates (keeps torch out of the Airflow image)."""

from datetime import datetime

import requests
from airflow.sdk import dag, task
from common import DEFAULT_ARGS, MARTS

from lakehouse.ops import instrumented

AGENTS_URL = "http://agents:8090"


@instrumented(source="agents", entity="fact_sheets")
def refresh() -> dict:
    r = requests.post(f"{AGENTS_URL}/fact-sheets/refresh", timeout=900)
    r.raise_for_status()
    out = r.json()
    return {"rows_read": out["documents"], "rows_written": out["chunks_embedded"], **out}


@dag(schedule=[MARTS], start_date=datetime(2026, 11, 1), catchup=False, default_args=DEFAULT_ARGS,
     max_active_runs=1, tags=["agents"])
def fact_sheets():
    @task
    def render_and_embed_changed() -> dict:
        return refresh()

    render_and_embed_changed()


fact_sheets()
