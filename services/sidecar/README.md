# BotSquadron sidecar

The sidecar calculates live NIFTY 50 puller/dragger values for active bots. It registers the 50 constituents from `files/official_nifty50_weights.csv` plus `NSE_INDEX|Nifty 50` with marketfeeder, consumes the existing NATS tick stream, builds exchange-time one-minute OHLC candles, persists each completed calculation to JSON, and serves the latest result over HTTP.

## Calculation

For constituent `i`:

```text
move_i   = stock_1m_close_i / stock_previous_close_i - 1
points_i = nifty_previous_close * weight_i / 100 * move_i
```

`puller_value` is the sum of positive `points_i`. `dragger_value` is the signed, negative sum of negative `points_i`, and `net_value = puller_value + dragger_value`. Classification thresholds and carry-forward behavior match `garageforbots/index/nifty50/nifty50_dragger_puller.py`.

The NATS full feed's `ltpc.cp` supplies the previous-session close. A snapshot is intentionally withheld until valid previous closes are present for all 50 stocks and NIFTY. Missing trades within a minute use only the last completed close, initially the previous-session close; future or still-open candles are never used.

The `datetime` and `timestamp` fields represent availability/end time. For example, the 09:15 candle produces a `09:16:00+05:30` snapshot.

## Run locally

```bash
export NATS_URL=nats://localhost:4222
export SIDECAR_OUTPUT_PATH=/tmp/botsquadron-sidecar/dragger_puller.json
cd services/sidecar
go run ./cmd
```

Important settings:

| Variable | Default | Purpose |
| --- | --- | --- |
| `SIDECAR_HTTP_ADDR` | `:8082` | HTTP listen address |
| `SIDECAR_WEIGHTS_PATH` | `files/official_nifty50_weights.csv` | Validated 50-stock weights file |
| `SIDECAR_OUTPUT_PATH` | `files/dragger_puller.json` | Atomic local JSON output |
| `SIDECAR_INDEX_INSTRUMENT_KEY` | `NSE_INDEX\|Nifty 50` | NIFTY index key needed for point scaling |
| `SIDECAR_FINALIZE_GRACE` | `2s` | Late-tick grace after a minute ends |
| `SIDECAR_MARKET_OPEN` / `SIDECAR_MARKET_CLOSE` | `09:15` / `15:30` | IST candle window |
| `SIDECAR_SUBSCRIPTION_REFRESH_INTERVAL` | `30s` | Re-register with non-durable marketfeeder state |
| `APP_TIMEZONE` | `Asia/Kolkata` | Candle and output timezone |

The sidecar republishes its complete instrument registration after NATS reconnects and every refresh interval. This is required because core NATS does not retain commands and marketfeeder keeps registrations only in memory.

## API

- `GET /healthz` — process liveness
- `GET /readyz` — NATS and previous-close readiness; returns 503 until exact calculations are possible
- `GET /v1/dragger-puller` — latest completed snapshot; returns 503 before the first snapshot
- `GET /v1/dragger-puller/history?limit=30` — versioned local document and recent snapshots

The latest response includes the requested fields (`datetime`, `dragger_value`, `puller_value`, and `market_classification`) plus NIFTY diagnostics, coverage/freshness, one-minute candle metadata, and per-stock point contributions.

Run tests with:

```bash
go test ./...
go test -race ./...
```
