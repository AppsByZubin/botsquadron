# Firebot

Firebot is a NIFTY 50 options bot cloned from `haemabot`. It now uses the
`bb_vwap_ema` strategy and the same runtime wiring as a starting point for
modification.

## Run Locally

```bash
cd bots/firebot
python main.py --instruments nifty50 --strategy bb_vwap_ema --level mock
```

## Runtime Wiring

- Market data arrives through NATS/marketfeeder.
- Orders route through the shared ordersystem client in `oms/`.
- Mock mode uses the local-only `MockOrderSystemClient`.
- Params load from `FIREBOT_PARAM_YAML`, `FIREBOT_PARAM_FILE`, or `files/param.yaml`.

## Key Environment

- `FIREBOT_MODE`: `mock`, `sandbox`, or `production`
- `FIREBOT_FILES_DIR`: runtime files directory, defaults to `files`
- `FIREBOT_PARAM_FILE`: YAML config path
- `FIREBOT_PARAM_YAML`: YAML config payload
- `NATS_URL`: marketfeeder NATS URL
- `UPSTOX_API_ACCESS_TOKEN`: Upstox token for market data/bootstrap
- `FIREBOT_UPLOAD_ARTIFACTS`: set to `true` to upload order artifacts to S3
