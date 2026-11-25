import json
from pathlib import Path

from pyspark.sql import Row

from spark_jobs.stream_market import parse, to_bars

FIXTURE = Path(__file__).parents[2] / "fixtures" / "binance_trades.jsonl"


def kafka_rows(spark, payloads):
    return spark.createDataFrame(
        [Row(value=p.encode(), partition=0, offset=i) for i, p in enumerate(payloads)]
    )


def test_parse_recorded_trades(spark):
    payloads = [json.dumps(json.loads(line)["data"]) for line in FIXTURE.read_text().splitlines()]
    out = parse(kafka_rows(spark, payloads)).collect()
    assert len(out) == len(payloads)
    assert all(r["_valid"] for r in out)
    first = json.loads(payloads[0])
    assert out[0]["symbol"] == first["s"] and out[0]["trade_id"] == first["t"]
    assert out[0]["price"] == float(first["p"])
    assert int(out[0]["trade_time"].timestamp() * 1000) == first["T"]


def test_parse_flags_bad_records(spark):
    out = parse(kafka_rows(spark, ["not json", json.dumps({"e": "trade", "s": "BTCUSDT", "t": 1, "p": "-1",
                                                           "q": "1", "T": 1, "E": 1, "m": False})])).collect()
    assert [r["_valid"] for r in out] == [False, False]


def test_bars_ohlcv(spark):
    base = 1_795_596_780_000  # ms, start of a minute
    trades = [("BTCUSDT", 1, "10", "1", base + 1_000), ("BTCUSDT", 2, "12", "1", base + 2_000),
              ("BTCUSDT", 3, "9", "2", base + 3_000)]
    payloads = [json.dumps({"e": "trade", "E": t, "s": s, "t": i, "p": p, "q": q, "T": t, "m": False})
                for s, i, p, q, t in trades]
    bars = to_bars(parse(kafka_rows(spark, payloads)).where("_valid")).collect()
    (b,) = bars
    assert (b["open"], b["high"], b["low"], b["close"], b["volume"], b["trade_count"]) == (10, 12, 9, 9, 4, 3)
    assert b["vwap"] == (10 + 12 + 18) / 4


def test_parse_equity_bars_from_producer_messages(spark):
    from datetime import timedelta

    from spark_jobs.stream_market import parse_equity
    from streaming import equity_producer as ep

    recs = json.loads((FIXTURE.parent / "yahoo_aapl_1m.json").read_text())
    last = ep.datetime.strptime(recs[-1]["Datetime"], ep.TIME_FMT)
    msgs = [json.dumps(m) for m in ep.releasable("AAPL", recs, None, last + timedelta(hours=1))]
    bad = json.dumps({"ticker": "AAPL", "minute": "2026-11-24T20:00:00+00:00", "open": 1.0, "high": 1.0,
                      "low": 2.0, "close": 1.5, "volume": 10, "published_at": 1, "delay_minutes": 10})
    out = parse_equity(kafka_rows(spark, [*msgs, bad])).collect()
    assert [r["_valid"] for r in out] == [True] * len(msgs) + [False]
    first = json.loads(msgs[0])
    assert out[0]["ticker"] == "AAPL" and out[0]["close"] == first["close"]
    # collect() returns naive local-time datetimes, so compare instants
    assert out[0]["minute"].timestamp() == ep.datetime.fromisoformat(first["minute"]).timestamp()
