"""The ticker universe. Single source of truth: dbt/seeds/universe.csv."""

import csv
from pathlib import Path

UNIVERSE_CSV = Path(__file__).resolve().parents[2] / "dbt" / "seeds" / "universe.csv"


def load(asset_class: str | None = None) -> list[dict]:
    with UNIVERSE_CSV.open() as f:
        rows = list(csv.DictReader(f))
    return [r for r in rows if asset_class is None or r["asset_class"] == asset_class]
