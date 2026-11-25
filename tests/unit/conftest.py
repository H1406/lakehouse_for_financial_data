from pathlib import Path

import pytest

FIXTURES = Path(__file__).parents[1] / "fixtures"


def fixture_or_skip(name: str) -> Path:
    p = FIXTURES / name
    if not p.exists():
        pytest.skip(f"{name} not recorded yet: run `python scripts/record_fixtures.py` with credentials in .env")
    return p
