# OrderSystem Service

`ordersystem` is a Go HTTP service that accepts trade-create requests from bots, stores trade lifecycle data in PostgreSQL, places production orders via Upstox, and polls Stop Loss (SL) order status.

## Features

- `POST /v1/trades` to create trade records from bots
- `POST /v1/trades/{id}/modify` to modify all SL broker orders for a trade
- Writes/updates PostgreSQL tables:
  - `trades`
  - `accounts`
  - `trades.account_id` links to `accounts.id` after the trade is closed
- In `APP_MODE=production`:
  - places entry order via Upstox Orders API
  - places SL order (when `sl_trigger` is provided)
  - periodically polls SL order status
  - closes trade in DB when SL is completed
  - updates daily `accounts.profit` and `accounts.max_drawdown`

## API Endpoints

- `GET /healthz`
- `POST /v1/trades`
- `POST /v1/trades/{id}/modify`
- `GET /v1/trades/{id}`

### Create Trade Request Example

```json
{
  "bot_name": "nifty50_pcr_vwap_ema_orb",
  "init_cash": 100000,
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
  "taxes": 0,
  "tag_entry": "bot-entry",
  "tag_sl": "bot-sl",
  "description": "PCR VWAP setup"
}
```

### Modify Trade Request Example

```json
{
  "mode": "production",
  "validity": "DAY",
  "order_type": "SL",
  "trigger_price": 91,
  "price": 90.5,
  "spot_trail_anchor": 22375
}
```

## Environment Variables

Required:

- `DATABASE_URL` e.g. `postgresql://user:pass@host:5432/omsdb?sslmode=disable`

Optional:

- `ORDERSYSTEM_HTTP_ADDR` default `:8081`
- `APP_MODE` default `mock` (`production` enables Upstox calls)
- `APP_TIMEZONE` default `Asia/Kolkata`
- `ORDERSYSTEM_REQUEST_TIMEOUT` default `15s`
- `ORDERSYSTEM_SL_POLL_INTERVAL` default `5s`
- `ACCOUNT_INITIAL_CASH` default `0`

Upstox:

- `UPSTOX_API_ACCESS_TOKEN` required when `APP_MODE=production`
- `UPSTOX_API_BASE_URL` default `https://api.upstox.com`
- `UPSTOX_ORDER_PLACE_PATH` default `/v3/order/place`
- `UPSTOX_ORDER_MODIFY_PATH` default `/v3/order/modify`
- `UPSTOX_ORDER_DETAILS_PATH` default `/v2/order/details`
- `UPSTOX_API_VERSION` default `2.0`

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
- Order status values: https://upstox.com/developer/api-documentation/appendix/order-status/
