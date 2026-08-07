# Meanbot

Meanbot is a NIFTY 50 options bot that runs the `meanrev_vwap` strategy. It
builds one-minute spot and futures candles, calculates session VWAP bands from
futures volume, and looks for mean-reversion entries when a completed candle
body is fully outside the configured band.

## Run Locally

```bash
conda deactivate
conda activate botsquadron
cd bots/meanbot
python main.py --instruments nifty50 --strategy meanrev_vwap --level mock
```

## Runtime Wiring

- Market data arrives through NATS/marketfeeder.
- Orders route through the shared ordersystem client in `oms/`.
- `block_bot()` pauses new order intake and `resume_bot()` re-enables it.
- Mock mode uses the local-only `MockOrderSystemClient`.
- Params load from `MEANBOT_PARAM_YAML`, `MEANBOT_PARAM_FILE`, or `files/param.yaml`.

## Key Environment

- `MEANBOT_MODE`: `rest`, `mock`, `sandbox`, or `production`
- `MEANBOT_FILES_DIR`: runtime files directory, defaults to `files`
- `MEANBOT_PARAM_FILE`: YAML config path
- `MEANBOT_PARAM_YAML`: YAML config payload
- `NATS_URL`: marketfeeder NATS URL
- `UPSTOX_API_ACCESS_TOKEN`: Upstox token for market data/bootstrap
- `DO_S3_*`: S3/Spaces configuration for uploading end-of-day artifacts
