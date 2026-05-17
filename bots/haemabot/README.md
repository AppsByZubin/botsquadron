# Haemabot

Haemabot is the NIFTY 50 options bot using the `hm_ema_adx` strategy ported from
the standalone `haemabot` repository.

## Run Locally

```bash
cd bots/haemabot
python main.py --instruments nifty50 --strategy hm_ema_adx --level mock
```

## Runtime Wiring

- Market data arrives through NATS/marketfeeder.
- Orders route through the shared ordersystem client in `oms/`.
- Mock mode uses the local-only `MockOrderSystemClient`.
- Params load from `HAEMABOT_PARAM_YAML`, `HAEMABOT_PARAM_FILE`, or `files/param.yaml`.

## Key Environment

- `HAEMABOT_MODE`: `mock`, `sandbox`, or `production`
- `HAEMABOT_FILES_DIR`: runtime files directory, defaults to `files`
- `HAEMABOT_PARAM_FILE`: YAML config path
- `HAEMABOT_PARAM_YAML`: YAML config payload
- `NATS_URL`: marketfeeder NATS URL
- `UPSTOX_API_ACCESS_TOKEN`: Upstox token for market data/bootstrap
- `HAEMABOT_UPLOAD_ARTIFACTS`: set to `true` to upload order artifacts to S3
