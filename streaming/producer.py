"""Binance trade WebSocket -> Redpanda topic `crypto_trades` (key = symbol).

Forwards the inner `data` object unchanged as JSON; parsing and validation happen in Spark.
Reconnects with exponential backoff (Binance drops connections after 24 h).
"""

import asyncio
import json
import logging
import os
import random

import websockets
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

logging.basicConfig(level=logging.INFO, format='{"ts":"%(asctime)s","level":"%(levelname)s","msg":"%(message)s"}')
log = logging.getLogger("producer")

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "redpanda:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "crypto_trades")
SYMBOLS = os.getenv("CRYPTO_SYMBOLS", "btcusdt,ethusdt,solusdt").split(",")
WS_URL = os.getenv("BINANCE_WS", "wss://stream.binance.com:9443") + "/stream?streams=" + "/".join(
    f"{s}@trade" for s in SYMBOLS
)


def ensure_topic() -> None:
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    if TOPIC not in admin.list_topics(timeout=10).topics:
        admin.create_topics([NewTopic(TOPIC, num_partitions=6, replication_factor=1)])[TOPIC].result()
        log.info("created topic %s", TOPIC)


async def run(producer: Producer) -> None:
    backoff = 1.0
    while True:
        try:
            async with websockets.connect(WS_URL, ping_interval=20) as ws:
                log.info("connected %s", WS_URL)
                backoff = 1.0
                async for raw in ws:
                    data = json.loads(raw).get("data")
                    if data is None:
                        continue
                    producer.produce(TOPIC, key=str(data.get("s", "")), value=json.dumps(data))
                    producer.poll(0)
        except Exception as exc:  # noqa: BLE001 - any disconnect -> reconnect
            sleep = min(backoff, 60) + random.random()
            log.warning("websocket error %s: %s; reconnecting in %.1fs", type(exc).__name__, exc, sleep)
            await asyncio.sleep(sleep)
            backoff *= 2


if __name__ == "__main__":
    ensure_topic()
    p = Producer({"bootstrap.servers": BOOTSTRAP, "acks": "all", "enable.idempotence": True, "linger.ms": 50})
    try:
        asyncio.run(run(p))
    finally:
        p.flush(10)
