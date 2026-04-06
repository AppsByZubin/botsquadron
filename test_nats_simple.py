#!/usr/bin/env python3
"""
Simple NATS connectivity test
"""
import asyncio
import nats
import os

async def test_nats():
    nats_url = os.getenv("NATS_URL", "nats://localhost:4222")

    try:
        # Connect to NATS
        nc = await nats.connect(nats_url)
        print("✅ Connected to NATS successfully")

        # Subscribe to a test subject
        messages = []

        async def message_handler(msg):
            messages.append(msg)
            print(f"📨 Received: {msg.data.decode()}")

        sub = await nc.subscribe("test.subject", cb=message_handler)
        print("✅ Subscribed to test.subject")

        # Publish a test message
        await nc.publish("test.subject", b"Hello from test!")
        print("✅ Published test message")

        # Wait a bit for the message to be received
        await asyncio.sleep(1)

        # Clean up
        await sub.unsubscribe()
        await nc.close()
        print("✅ Test completed successfully")

        if len(messages) > 0:
            print(f"✅ Received {len(messages)} message(s)")
        else:
            print("⚠️  No messages received (NATS server might not be running)")

    except Exception as e:
        print(f"❌ Test failed: {e}")

if __name__ == "__main__":
    asyncio.run(test_nats())