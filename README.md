# BotSquadron - Trading Bot Platform

BotSquadron is a distributed trading bot platform that uses NATS for communication between trading bots (solobot) and market data feeders (marketfeeder).

## Architecture

### Components

1. **solobot** (Python): Trading bot that implements trading strategies
2. **ordersystem** (Go): HTTP OMS that stores trades in PostgreSQL, places production orders via Upstox, and polls SL status
3. **marketfeeder** (Go): Market data feeder that connects to Upstox **v3** websockets
4. **NATS**: Message broker for communication between components

### Communication Flow

```
solobot → ordersystem → PostgreSQL
solobot → NATS → marketfeeder → Upstox WebSocket → NATS → solobot
ordersystem (production mode) → Upstox Orders API
```

1. solobot sends instrument keys to subscribe to marketfeeder via NATS
2. marketfeeder creates Upstox websocket connections for those instruments
3. marketfeeder receives tick data from Upstox and publishes it back to NATS
4. solobot receives tick data from NATS and processes it for trading decisions

## Setup

### Prerequisites

- Go 1.21+
- Python 3.8+
- NATS server running
- Upstox API access token

### Environment Variables

Set the following environment variables:

```bash
export NATS_URL="nats://localhost:4222"
export UPSTOX_API_ACCESS_TOKEN="your_upstox_token"
```

### Installation

1. **Install Python dependencies:**
   ```bash
   make install-python-deps
   ```

2. **Build marketfeeder:**
   ```bash
   make build
   ```

3. **Build Docker image:**
   ```bash
   make docker-build
   ```

4. **Build ordersystem:**
   ```bash
   make build-ordersystem
   ```

## Usage

### Running NATS Server

```bash
# Using Docker
docker run -p 4222:4222 -p 8222:8222 nats:latest

# Or install and run locally
nats-server -DV

# Stop the local NATS server
# - If started in the foreground, use Ctrl+C
# - If running in the background, use the PID from the process list
kill $(pgrep nats-server)
```

### Running marketfeeder

```bash
# Local
./services/marketfeeder/marketfeeder

# Docker
docker run -e NATS_URL=nats://host.docker.internal:4222 -e UPSTOX_API_ACCESS_TOKEN=your_token marketfeeder:latest
```

**Note:** marketfeeder is configured to use Upstox **v3 websocket API**. Please verify the endpoint URL, authentication, and message formats with the official Upstox v3 documentation, as the implementation may need adjustments based on the actual API specifications.

### Running ordersystem

```bash
export DATABASE_URL="postgresql://omsuser:change-me@localhost:5432/omsdb?sslmode=disable"
export APP_MODE="mock"   # use production to enable Upstox order placement
./services/ordersystem/ordersystem
```

Or from source:

```bash
cd services/ordersystem
go run ./cmd
```

API docs and examples:

`services/ordersystem/README.md`

### Running solobot

```bash
cd bots/solobot
python -m index.orchestrator nifty50 sma production
```

### Testing NATS Communication

```bash
make test-nats
```

## NATS Message Formats

### Instrument Subscription

**Subject:** `marketfeeder.instrument_keys`

```json
{
  "bot_id": "nifty50_sma_production_1640995200",
  "instrument_keys": ["NSE_EQ|INE002A01018", "NSE_EQ|INE009A01021"],
  "action": "subscribe"
}
```

### Add Instruments

**Subject:** `marketfeeder.add_instruments`

```json
{
  "bot_id": "nifty50_sma_production_1640995200",
  "instrument_keys": ["NSE_EQ|INE003A01019"],
  "action": "add"
}
```

### Remove Instruments

**Subject:** `marketfeeder.remove_instruments`

```json
{
  "bot_id": "nifty50_sma_production_1640995200",
  "instrument_keys": ["NSE_EQ|INE002A01018"],
  "action": "remove"
}
```

### Tick Data

**Subject:** `marketfeeder.tick_data`

```json
{
  "instrument_key": "NSE_EQ|INE002A01018",
  "price": 1500.50,
  "volume": 1000,
  "timestamp": "2024-01-01T10:30:00Z"
}
```

## Kubernetes Deployment

The platform can be deployed to Kubernetes:

1. **Deploy NATS:**
   ```bash
   kubectl apply -f k8s/nats-deployment.yaml
   ```

2. **Create Upstox secret:**
   ```bash
   kubectl create secret generic upstox-secrets --from-literal=api-token=your_token
   ```

3. **Deploy marketfeeder:**
   ```bash
   kubectl apply -f k8s/marketfeeder-deployment.yaml
   ```

## Development

### Adding New Bots

1. Create a new engine in `bots/solobot/index/`
2. Implement the NATS communication pattern
3. Update the orchestrator to route to your new engine

### Modifying Market Data

The marketfeeder can be extended to support multiple brokers by:

1. Adding new websocket handlers
2. Implementing broker-specific message parsing
3. Publishing standardized tick data format

## Monitoring

- NATS server provides built-in monitoring at `http://localhost:8222`
- Logs are available in `bots/solobot/logs/` for solobot
- marketfeeder logs to stdout/stderr

## Troubleshooting

### Common Issues

1. **NATS connection failed:** Ensure NATS server is running and accessible
2. **Upstox websocket connection failed:** Check API token and network connectivity
3. **No tick data received:** Verify instrument keys are valid and market is open
4. **v3 API compatibility:** The marketfeeder uses Upstox v3 websocket API. If you encounter connection or message parsing issues, please check the official Upstox v3 websocket documentation for any required changes to authentication, message formats, or endpoint URLs

### Debug Mode

Enable debug logging by setting log level in the respective components.
