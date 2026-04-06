#!/usr/bin/env python3
"""strategy-sma NATS subscriber test.

Run this script on your local machine while marketfeeder is publishing to market.data.
"""

import asyncio
import os

from nats.aio.client import Client as NATS


def get_nats_url():
    return os.getenv("NATS_URL", "nats://127.0.0.1:4222")


async def run():
    nc = NATS()
    await nc.connect(get_nats_url())
    print(f"Connected to NATS at {nc.connected_url.netloc}")

    async def message_handler(msg):
        subject = msg.subject
        data = msg.data.decode()
        print(f"[strategy-sma] Received on {subject}: {data}")

    await nc.subscribe("market.data", cb=message_handler)
    print("Subscribed to 'market.data', waiting for messages...")

    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        print("Shutting down subscriber...")
    finally:
        await nc.drain()
        await nc.close()


if __name__ == "__main__":
    asyncio.run(run())
