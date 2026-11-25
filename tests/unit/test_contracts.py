import json

from lakehouse.ingestion.contracts import BinanceTrade

from .conftest import fixture_or_skip


def test_recorded_binance_trades_match_contract():
    lines = fixture_or_skip("binance_trades.jsonl").read_text().splitlines()
    trades = [BinanceTrade.model_validate(json.loads(line)["data"]) for line in lines]
    assert trades and all(t.e == "trade" and float(t.p) > 0 for t in trades)
