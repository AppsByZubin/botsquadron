# Trendobot

Trendobot is a Python trading bot focused on NIFTY 50 option strategies inside
BotSquadron. The current live engine routes `nifty50` through the `vwma_ema_st`
strategy, subscribes to ticks through NATS/marketfeeder, and manages trades
through the shared ordersystem client.

This project is automation tooling for trading research and execution. Review
the strategy, parameters, and order-manager behavior carefully before using
real capital.

## What It Does

- Subscribes to marketfeeder ticks over NATS.
- Builds NIFTY 50 index and futures candles from live ticks.
- Selects relevant option contracts around ATM/ITM strikes.
- Runs the VWMA/EMA/Supertrend strategy.
- Places and manages orders through the BotSquadron ordersystem client.
- Writes order logs, event logs, and daily PnL outputs under `files/execution_results/`.
- Keeps local OMS ledger copies for mock/debugging.

## Project Layout

```text
trendobot/
├── main.py                         # CLI entrypoint
├── index/orchestrator.py           # Top-level instrument/strategy router
├── index/nifty50/nifty50_engine.py # NIFTY 50 live engine and NATS loop
├── index/nifty50/strategy/         # Strategy implementations
├── oms/                            # BotSquadron ordersystem client
├── order_manager/                  # Legacy production managers kept for reference
├── broker/                         # Upstox helper/wrapper
├── technicals/                     # Indicator helpers
├── utils/                          # Shared utilities
├── files/param.yaml                # Runtime strategy parameters
└── files/execution_results/        # Generated order/PnL logs
```

## Requirements

- Python 3.10+
- Upstox API credentials
- NATS and BotSquadron marketfeeder/ordersystem services
- TA-Lib native library installed on the system
- Python packages from `requirements.txt`

Install Python dependencies:

```bash
python3 -m pip install -r requirements.txt
```

If `TA-Lib` fails to install, install the native TA-Lib package for your OS
first, then rerun the Python dependency install.

## Configuration

Strategy parameters live in:

```text
files/param.yaml
```

In Kubernetes, parameters can be supplied through `TRENDOBOT_PARAM_YAML`,
`TRENDOBOT_PARAMS_YAML`, `TRENDOBOT_PARAM_DATA`, or the existing solobot
compatible env names used by the BotSquadron Helm chart.

Common settings include:

- `trade-per-day`
- `max-open-trades`
- `take-profit`
- `stop-loss`
- `sl-limit-gap`
- `trade_expiry`
- `itm_strike_range`
- `lot-size`
- `trade-window`
- `historical-trends`

Upstox credentials are read from environment variables:

```bash
export upstox_api_access_token="..."
export upstox_sandbox_api_access_token="..."
```

The production token is required for market data. Sandbox token is additionally
required when running with `-l sandbox`.

NATS and ordersystem are read from the same env names used by solobot:

```bash
export NATS_URL="nats://localhost:4222"
export ORDERSYSTEM_BASE_URL="http://localhost:8081"
```

## Run

Mock mode:

```bash
python3 main.py -i nifty50 -s vwma_ema_st -l mock
```

Sandbox mode:

```bash
python3 main.py -i nifty50 -s vwma_ema_st -l sandbox
```

Production mode:

```bash
python3 main.py -i nifty50 -s vwma_ema_st -l production
```

Valid execution modes are:

- `mock`
- `sandbox`
- `production`

Valid strategy currently wired through the NIFTY 50 engine:

- `vwma_ema_st`

## Output Files

Execution artifacts are written under mode-specific folders:

```text
files/execution_results/mock/
files/execution_results/sandbox/
files/execution_results/prod/
```

Typical outputs:

- `order_log.csv`
- `order_event_log.json`
- `order_status_log.csv`
- `daily_pnl.csv`

## VWMA/EMA/Supertrend Strategy Notes

The `vwma_ema_st` strategy:

- Uses VWMA, EMA, RSI moving average, ATR, and Supertrend filters.
- Enters CALL/PUT option trades based on directional setup and configured bias.
- Normalizes marketfeeder messages before candle building and trade processing.
- Manages open trades through the configured ordersystem client.

## Development Checks

Compile a changed file:

```bash
python3 -X pycache_prefix=/tmp/codex-pyc -m py_compile index/nifty50/strategy/vwma_ema_st.py
```

Run a quick import/compile sweep as needed before live use.

## Safety Notes

- Keep access tokens out of source control.
- Use `mock` mode first when changing strategy or order-manager code.
- Confirm `files/param.yaml` expiry dates before each trading session.
- Review generated order logs after every run.
- Production mode can place real orders.
