# Fibobot

Fibobot is a NIFTY 50 options bot that runs the `fib_ema_atr_vol` strategy:
Fibonacci pivots, EMA trend confirmation, ATR risk, and optional volume filters
on confirmed intraday index candles.

## Run Locally

```bash
cd bots/fibobot
python main.py --instruments nifty50 --strategy fib_ema_atr_vol --level mock
```

## Runtime Wiring

- Market data arrives through NATS/marketfeeder.
- Orders route through the shared ordersystem client in `oms/`.
- Mock mode uses the local-only `MockOrderSystemClient`.
- Params load from `FIBOBOT_PARAM_YAML`, `FIBOBOT_PARAM_FILE`, or `files/param.yaml`.

## Key Environment

- `FIBOBOT_MODE`: `mock`, `sandbox`, or `production`
- `FIBOBOT_FILES_DIR`: runtime files directory, defaults to `files`
- `FIBOBOT_PARAM_FILE`: YAML config path
- `FIBOBOT_PARAM_YAML`: YAML config payload
- `NATS_URL`: marketfeeder NATS URL
- `UPSTOX_API_ACCESS_TOKEN`: Upstox token for market data/bootstrap
- `DO_S3_*`: S3/Spaces configuration for uploading end-of-day artifacts
