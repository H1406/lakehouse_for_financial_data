# Runbook: stream stalled

Scope: the crypto stream. The delayed equity feed (`equity_bars`) is legitimately silent outside US market hours.

**Signals:** `check_stream_health` failed ("no stream events in the last 5 minutes"); no new rows in `ops.stream_metrics`.

## Source or Spark? Read the heartbeats first
`ops.stream_metrics` gets a 0-event **idle heartbeat** row (`batch_id = -1`) about once a minute per query while Spark
is alive but Kafka has nothing new:
- heartbeats continue, no events → **source stalled** (producer stopped, WebSocket blocked); Spark is fine.
- no rows at all (no events, no heartbeats) → **Spark job down** (container crashed, OOM, catalog errors).

## Diagnose (source first, then Spark)
1. Producer: `docker logs --tail 50 lakehouse-producer-1`. Reconnect loops / `websocket error` → Binance side or network. Container not running → it was stopped.
2. Broker: `docker exec lakehouse-redpanda-1 rpk topic describe crypto_trades -p` — are high watermarks still increasing?
   - Offsets growing but no metrics → Spark is the problem (step 3).
   - Offsets flat → the producer / source is the problem.
3. Spark: `docker logs --tail 100 lakehouse-spark-stream-1 | grep -E 'Exception|Caused by'`; `docker inspect -f '{{.RestartCount}}' lakehouse-spark-stream-1`.

## Fix
- Producer stopped → `make stream-start`. Binance blocked → set `BINANCE_WS` to `wss://data-stream.binance.vision`.
- Spark crash loop → read the first `Caused by`. `SQLITE_BUSY` / `CommitStateUnknown` → catalog contention (see DECISIONS). A replayed batch is safe: Silver dedups by `(symbol, trade_id)`.
- After recovery, verify: `python scripts/check_stream_continuity.py --since <outage start>` shows `missing: 0` in Silver.
