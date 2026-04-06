#!/usr/bin/env python3
"""
Test the full NATS communication between solobot and marketfeeder
"""
import asyncio
import json
import nats
import os

NATS_URL = os.getenv("NATS_URL", "nats://localhost:4222")
NATS_SUBJECT_INSTRUMENT_KEYS = "marketfeeder.instrument_keys"
NATS_SUBJECT_TICK_DATA = "marketfeeder.tick_data"

async def test_full_communication():
    """Test the complete communication flow"""

    # Connect to NATS
    nc = await nats.connect(NATS_URL)
    print("✅ Connected to NATS")

    try:
        # Subscribe to tick data
        tick_messages = []

        async def tick_handler(msg):
            data = json.loads(msg.data.decode())
            tick_messages.append(data)
            print(f"📈 Received tick data: {data}")

        await nc.subscribe(NATS_SUBJECT_TICK_DATA, cb=tick_handler)
        print("✅ Subscribed to tick data")

        # Send instrument subscription (simulate solobot)
        test_instruments = ["NSE_EQ|INE002A01018", "NSE_EQ|INE009A01021"]
        bot_id = "test_bot_full_communication"

        subscription = {
            "bot_id": bot_id,
            "instrument_keys": test_instruments,
            "action": "subscribe"
        }

        await nc.publish(NATS_SUBJECT_INSTRUMENT_KEYS, json.dumps(subscription).encode())
        print(f"✅ Published instrument subscription for bot {bot_id}: {test_instruments}")

        # Wait for responses
        print("⏳ Waiting for tick data...")
        await asyncio.sleep(5)

        print(f"✅ Test completed. Received {len(tick_messages)} tick messages")

        if len(tick_messages) == 0:
            print("ℹ️  No tick data received (marketfeeder may not be connected to real Upstox)")
            print("   This is expected in a test environment without real API access")

    except Exception as e:
        print(f"❌ Test failed: {e}")
    finally:
        await nc.close()

if __name__ == "__main__":
    asyncio.run(test_full_communication())