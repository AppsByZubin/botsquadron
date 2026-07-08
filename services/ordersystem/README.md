# OrderSystem Service

`ordersystem` is a Go HTTP service that accepts trade-create requests from bots, stores trade lifecycle data in PostgreSQL, places broker orders via Upstox in `production`/`sandbox`, and polls Stop Loss (SL) order status only in `production`.

## Features

- `POST /v1/accounts` to idempotently prepare a daily account row for a bot
- `POST /v1/trades` to create trade records from bots
- `POST /v1/trades/{id}/modify` to modify all SL broker orders for a trade
- `POST /v1/trades/{id}/square-off` to square off a trade from strategy code
- `POST /v1/bots/{bot_name}/block-orders` to block new order requests for a bot without touching existing broker/OMS trades
- `POST /v1/bots/{bot_name}/kill` to block new orders for a bot without touching existing broker/OMS trades
- `POST /v1/bots/{bot_name}/resume` to disable kill mode for a bot
- Writes/updates PostgreSQL tables:
  - `accounts`
  - `trades`
  - `orders`
  - `trades.acct_id` links to `accounts.id`
  - broker entry/SL ids and exchange order ids are stored as one row per order in `orders`
- In `APP_MODE=production`:
  - uses `UPSTOX_API_BASE_URL`
  - places entry order via Upstox Orders API
  - places SL order (when `sl_trigger` is provided)
  - periodically polls SL order status
  - syncs actual filled entry/exit prices from Upstox order details
  - calculates per-order and trade-level brokerage from Upstox charges
  - closes trade in DB when SL is completed
  - blocks new bot orders while kill mode is enabled
  - updates daily `accounts.net_profit`
- In `APP_MODE=sandbox`:
  - uses `UPSTOX_SANDBOX_API_BASE_URL`
  - places entry/SL orders and trailing SL modifications via Upstox sandbox
  - does not call Upstox order details/trades APIs, because sandbox does not support order details
  - relies on the strategy to call the square-off endpoint with the latest exit price

## API Endpoints

- `GET /healthz`
- `POST /v1/accounts`
- `GET /v1/accounts?bot_name=<bot>&curr_date=<DD-MM-YYYY>`
- `POST /v1/trades`
- `POST /v1/trades/{id}/modify`
- `POST /v1/trades/{id}/square-off`
- `GET /v1/trades/{id}`
- `POST /v1/bots/{bot_name}/block-orders`
- `GET /v1/bots/{bot_name}/block-orders`
- `POST /v1/bots/{bot_name}/kill`
- `POST /v1/bots/{bot_name}/resume`
- `GET /v1/bots/{bot_name}/kill`

### Create Account Request Example

Repeated calls with the same `bot_name` and `curr_date` return the same account row.

```json
{
  "bot_name": "nifty50_pcr_vwap_ema_orb",
  "curr_date": "14-04-2026",
  "init_cash": 100000
}
```

Response includes account fields only:

- `account_id`, `bot_name`, `curr_date`, `month_year`, `init_cash`, `net_profit`

### Get Account Details

Use this after a bot restart to resync the bot's local state from OMS.

```bash
curl 'http://localhost:8081/v1/accounts?bot_name=nifty50_pcr_vwap_ema_orb&curr_date=14-04-2026'
```

Response includes account fields plus `trades`, with each trade carrying its nested `orders`.

### Create Trade Request Example

Trade creation also prepares the daily account row for the bot before storing the trade.

```json
{
  "bot_name": "nifty50_pcr_vwap_ema_orb",
  "init_cash": 100000,
  "curr_date": "14-04-2026",
  "month_year": "042026",
  "mode": "production",
  "symbol": "NIFTY24APR23500CE",
  "instrument_token": "NSE_FO|12345",
  "side": "BUY",
  "qty": 75,
  "product": "D",
  "validity": "DAY",
  "entry_price": 102.5,
  "target": 130,
  "sl_trigger": 90,
  "sl_limit": 89.5,
  "spot_trail_anchor": 22350,
  "total_brokerage": 0,
  "tag_entry": "bot-entry",
  "tag_sl": "bot-sl",
  "description": "PCR VWAP setup"
}
```

Response includes the legacy ID arrays plus structured broker refs:

```json
{
  "trade_id": "0f1e2d3c-4b5a-6789-9012-abcdefabcdef",
  "status": "OPEN",
  "entry_order_ids": ["250602010001234"],
  "sl_order_ids": ["250602010001235"],
  "entry_orders": [
    {"order_id": "250602010001234", "exchange_order_id": "1300000025660919"}
  ],
  "sl_orders": [
    {"order_id": "250602010001235", "exchange_order_id": "1300000025660920"}
  ]
}
```

In production, ordersystem places the entry/SL orders and then uses Upstox order-details to hydrate `exchange_order_id` best-effort. If Upstox has not assigned or returned it yet, the response still includes `order_id` and leaves `exchange_order_id` empty rather than retrying a placed order. Bot OMS clients persist these refs into their local order CSV (`entry_order_refs`, `sl_order_refs`) and JSON event logs (`entry_order_refs`, `sl_order_refs`, `entry_exchange_order_ids`, `sl_exchange_order_ids`).

### Modify Trade Request Example

```json
{
  "mode": "production",
  "validity": "DAY",
  "order_type": "SL",
  "stoploss": 91,
  "sl_limit": 90.5,
  "spot_trail_anchor": 22375
}
```

Validation:

- At least one of `stoploss`, `sl_limit`, or `spot_trail_anchor` is required.
- Provided price fields must be greater than `0`.
- `validity` must be `DAY` or `IOC`.
- `order_type` must be `SL` or `SL-M`.
- In sandbox/production mode, `stoploss` is required; `SL` orders also require `sl_limit`.
- If Upstox rejects an SL modification, ordersystem refreshes that SL order's broker status before failing the request. When the SL is already filled/closed, ordersystem records the trade as `STOPLOSS HIT` and returns `closed_trade`/`closed_trades` so the bot can sync its local ledger immediately.

### Square-Off Trade Request Example

The strategy owns square-off timing and sends the latest LTP as `exit_price`.

```json
{
  "mode": "sandbox",
  "exit_price": 108.25,
  "exit_time": "2026-04-20T15:10:00+05:30",
  "reason": "EOD_SQUARE_OFF"
}
```

### Kill Bot Request Example

Kill mode is stored in `ordersystem`, so future `POST /v1/trades` calls for the same bot return `Order intake blocked; no orders to be accepted.` Existing broker orders and OMS trade rows are not changed by this endpoint; handle those manually at the broker and DB if needed.

`block-orders` is an alias-style endpoint for the same intake-only behavior:

```bash
curl -X POST 'http://localhost:8081/v1/bots/haemabot/block-orders' \
  -H 'Content-Type: application/json' \
  -d '{"reason":"manual order intake block"}'
```

The helper script wraps this call:

```bash
scripts/ordersystem_order_intake_control.sh haemabot block
scripts/ordersystem_order_intake_control.sh haemabot status
scripts/ordersystem_order_intake_control.sh haemabot resume
scripts/ordersystem_order_intake_control.sh fibobot block
scripts/ordersystem_order_intake_control.sh fibobot status
scripts/ordersystem_order_intake_control.sh fibobot resume
```

```bash
curl -X POST 'http://localhost:8081/v1/bots/nifty50_pcr_vwap_ema_orb/kill' \
  -H 'Content-Type: application/json' \
  -d '{"curr_date":"14-04-2026","segment":"NSE_FO","reason":"manual kill"}'
```

Resume the bot:

```bash
curl -X POST 'http://localhost:8081/v1/bots/nifty50_pcr_vwap_ema_orb/resume'
```

## Environment Variables

Required:

- `DATABASE_URL` e.g. `postgresql://user:pass@host:5432/omsdb?sslmode=disable`

Optional:

- `ORDERSYSTEM_HTTP_ADDR` default `:8081`
- `APP_MODE` default `sandbox` (allowed values: `sandbox`, `production`)
- `APP_TIMEZONE` default `Asia/Kolkata`
- `ORDERSYSTEM_REQUEST_TIMEOUT` default `15s`
- `ORDERSYSTEM_SL_POLL_INTERVAL` default `10s`
- `ORDERSYSTEM_SL_REFRESH_MIN_INTERVAL` default `10s`
- `ACCOUNT_INITIAL_CASH` default `0`

Upstox:

- `UPSTOX_API_ACCESS_TOKEN` required when `APP_MODE=production`
- `UPSTOX_SANDBOX_API_ACCESS_TOKEN` required when `APP_MODE=sandbox` (`upstox_sandbox_api_access_token` is also accepted)
- `UPSTOX_API_BASE_URL` default `https://api.upstox.com`; production order endpoints below are resolved to their documented FQDNs even when env overrides are relative paths
- `UPSTOX_SANDBOX_API_BASE_URL` default `https://api-sandbox.upstox.com` and used in sandbox
- `UPSTOX_ORDER_PLACE_PATH` default `https://api-hft.upstox.com/v3/order/place` in production, `/v3/order/place` in sandbox
- `UPSTOX_ORDER_MODIFY_PATH` default `https://api-hft.upstox.com/v3/order/modify` in production, `/v3/order/modify` in sandbox
- `UPSTOX_ORDER_CANCEL_PATH` default `https://api-hft.upstox.com/v3/order/cancel` in production, `/v3/order/cancel` in sandbox
- `UPSTOX_EXIT_POSITIONS_PATH` default `https://api.upstox.com/v2/order/positions/exit` in production, `/v2/order/positions/exit` in sandbox
- `UPSTOX_ORDER_DETAILS_PATH` default `https://api.upstox.com/v2/order/details` in production, `/v2/order/details` in sandbox
- `UPSTOX_ORDER_TRADES_PATH` default `https://api.upstox.com/v2/order/trades` in production, `/v2/order/trades` in sandbox
- `UPSTOX_BROKERAGE_PATH` default `https://api.upstox.com/v2/charges/brokerage` in production, `/v2/charges/brokerage` in sandbox
- `UPSTOX_API_VERSION` default `2.0`
- `ORDERSYSTEM_UPSTOX_ORDER_REQUEST_GAP` default `750ms`
- `ORDERSYSTEM_UPSTOX_STATUS_REQUEST_GAP` default `750ms`
- `ORDERSYSTEM_UPSTOX_STATUS_CACHE_TTL` default `5s`

Bot OMS client rate-limit controls:

- `ORDERSYSTEM_MODIFY_MIN_INTERVAL_SEC` default `1.0`
- `ORDERSYSTEM_RATE_LIMIT_MAX_RETRIES` default `2`
- `ORDERSYSTEM_RATE_LIMIT_BASE_SLEEP_SEC` default `max(ORDERSYSTEM_MODIFY_MIN_INTERVAL_SEC, 1.0)`
- `ORDERSYSTEM_RATE_LIMIT_MAX_SLEEP_SEC` default `15.0`

## Run

```bash
cd services/ordersystem
go run ./cmd
```

## Build

```bash
cd services/ordersystem
go build -o ordersystem ./cmd
```

## Upstox References

- Orders API: https://upstox.com/developer/api-documentation/orders
- Exit All Positions API: https://upstox.com/developer/api-documentation/exit-all-positions/
- Cancel Order V3 API: https://upstox.com/developer/api-documentation/v3/cancel-order
- Order Details API: https://upstox.com/developer/api-documentation/get-order-details/
- Brokerage API: https://upstox.com/developer/api-documentation/get-brokerage/
- Order status values: https://upstox.com/developer/api-documentation/appendix/order-status/
