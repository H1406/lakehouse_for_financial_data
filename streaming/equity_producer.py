"""Delayed equity feed: yfinance 1-minute bars -> Redpanda topic `equity_bars_1m` (key = ticker).

Simulates a free-tier market-data feed (e.g. TradingView without a real-time subscription): a bar is published
only once it is complete *and* at least QUOTE_DELAY_MINUTES (default 10) old. Downstream is real streaming
(continuous Kafka events, event-time processing in Spark); only the source is deliberately delayed.

Why polling 1m history instead of a tick WebSocket: Yahoo keeps days of 1-minute history, so after a restart
the producer re-reads STARTUP_PERIOD and refills its own gap. Re-published bars are harmless because Spark
MERGEs Silver on (ticker, minute). One request per ticker per poll (12/min) keeps us gentle with Yahoo.
"""

import csv
import json
import logging
import math
import os
import time
from datetime import UTC, datetime, timedelta

import yfinance as yf
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

logging.basicConfig(level=logging.INFO, format='{"ts":"%(asctime)s","level":"%(levelname)s","msg":"%(message)s"}')
log = logging.getLogger("equity_producer")

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "redpanda:9092")
TOPIC = os.getenv("EQUITY_TOPIC", "equity_bars_1m")
DELAY = timedelta(minutes=int(os.getenv("QUOTE_DELAY_MINUTES", "10")))
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "60"))
STARTUP_PERIOD = os.getenv("STARTUP_PERIOD", "5d")  # how far back to refill after a (re)start
UNIVERSE_CSV = os.getenv("UNIVERSE_CSV", "/app/universe.csv")
TIME_FMT = "%Y-%m-%dT%H:%M:%S%z"
yf.config.debug.hide_exceptions = False


def tickers() -> list[str]:
    with open(UNIVERSE_CSV) as f:
        return [r["ticker"] for r in csv.DictReader(f) if r["asset_class"] == "equity"]


def to_records(df) -> list[dict]:
    """Same staging format as lakehouse.ingestion.yahoo.to_records (this container does not ship that package)."""
    out = []
    for ts, row in df.iterrows():
        rec = {"Datetime": ts.strftime(TIME_FMT)}
        for k, v in row.items():
            v = v.item() if hasattr(v, "item") else v
            rec[k] = None if isinstance(v, float) and math.isnan(v) else v
        out.append(rec)
    return out


def releasable(ticker: str, records: list[dict], last_sent: datetime | None, now: datetime) -> list[dict]:
    """Bars that are complete, at least DELAY old, and newer than the last one sent, as Kafka messages."""
    out = []
    for r in records:
        minute = datetime.strptime(r["Datetime"], TIME_FMT)
        if minute + timedelta(minutes=1) + DELAY > now or (last_sent and minute <= last_sent):
            continue
        out.append({
            "ticker": ticker,
            "minute": minute.astimezone(UTC).isoformat(),
            "open": r.get("Open"), "high": r.get("High"), "low": r.get("Low"), "close": r.get("Close"),
            "volume": None if r.get("Volume") is None else int(r["Volume"]),
            "published_at": int(now.timestamp() * 1000),
            "delay_minutes": int(DELAY.total_seconds() // 60),
        })
    return out


def ensure_topic() -> None:
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    if TOPIC not in admin.list_topics(timeout=10).topics:
        admin.create_topics([NewTopic(TOPIC, num_partitions=6, replication_factor=1)])[TOPIC].result()
        log.info("created topic %s", TOPIC)


def run(producer: Producer) -> None:
    last_sent: dict[str, datetime] = {}
    period = STARTUP_PERIOD
    universe = tickers()
    log.info("delayed feed for %d tickers, delay=%s, poll=%ss", len(universe), DELAY, POLL_SECONDS)
    while True:
        started = time.monotonic()
        now = datetime.now(UTC)
        sent = 0
        for t in universe:
            try:
                df = yf.Ticker(t).history(period=period, interval="1m", auto_adjust=False, actions=False,
                                          prepost=False)
            except Exception as exc:  # noqa: BLE001 - one bad ticker/poll must not stop the feed
                log.warning("fetch %s failed: %s: %s", t, type(exc).__name__, str(exc)[:200])
                continue
            msgs = releasable(t, to_records(df), last_sent.get(t), now)
            for m in msgs:
                producer.produce(TOPIC, key=t, value=json.dumps(m))
            if msgs:
                last_sent[t] = datetime.fromisoformat(msgs[-1]["minute"])
                sent += len(msgs)
            producer.poll(0)
            time.sleep(0.2)
        producer.flush(10)
        if sent:
            log.info("published %d bars (period=%s)", sent, period)
        period = "1d"
        time.sleep(max(0.0, POLL_SECONDS - (time.monotonic() - started)))


if __name__ == "__main__":
    ensure_topic()
    p = Producer({"bootstrap.servers": BOOTSTRAP, "acks": "all", "enable.idempotence": True, "linger.ms": 50})
    try:
        run(p)
    finally:
        p.flush(10)
