# Titanbot

Titanbot is a NIFTY 50 options bot that runs the `timeseries_trend` strategy:
1-minute candles rolled into 5-minute and 10-minute EMA-angle confirmation,
with firebot-style OMS, risk, and artifact wiring.

## Run Locally

```bash
cd bots/titanbot
python main.py --instruments nifty50 --strategy timeseries_trend --level mock
```

## Runtime Wiring

- Market data arrives through NATS/marketfeeder.
- Orders route through the shared ordersystem client in `oms/`.
- Mock mode uses the local-only `MockOrderSystemClient`.
- Params load from `TITANBOT_PARAM_YAML`, `TITANBOT_PARAM_FILE`, or `files/param.yaml`.

## Key Environment

- `TITANBOT_MODE`: `mock`, `sandbox`, or `production`
- `TITANBOT_FILES_DIR`: runtime files directory, defaults to `files`
- `TITANBOT_PARAM_FILE`: YAML config path
- `TITANBOT_PARAM_YAML`: YAML config payload
- `NATS_URL`: marketfeeder NATS URL
- `UPSTOX_API_ACCESS_TOKEN`: Upstox token for market data/bootstrap
- `DO_S3_*`: S3/Spaces configuration for uploading end-of-day artifacts
