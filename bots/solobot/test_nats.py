#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
==================================================
 File:        test_nats.py
 Author:      Amit Mohanty

 Notes:
    - Test script to verify NATS communication between solobot and marketfeeder
==================================================
"""

import asyncio
import json
import os
import sys

# Add current directory to sys.path
sys.path.insert(0, '.')

from common import constants
from logger import create_logger
import nats

logger = create_logger("NATSTestLogger")

async def test_nats_communication():
    """Test NATS communication between solobot and marketfeeder"""
    
    # Connect to NATS
    nc = await nats.connect(constants.NATS_URL)
    logger.info("Connected to NATS for testing")

    try:
        # Test data
        test_instruments = ["NSE_EQ|INE002A01018", "NSE_EQ|INE009A01021"]  # Sample instrument keys
        bot_id = "test_bot_123"

        # Subscribe to tick data first
        tick_count = 0
        async def tick_handler(msg):
            nonlocal tick_count
            try:
                tick_data = json.loads(msg.data.decode())
                logger.info(f"Received test tick data: {tick_data}")
                tick_count += 1
            except Exception as e:
                logger.error(f"Error in tick handler: {e}")

        await nc.subscribe(constants.NATS_SUBJECT_TICK_DATA, cb=tick_handler)
        logger.info("Subscribed to tick data")

        # Send instrument subscription
        subscription_data = {
            'bot_id': bot_id,
            'instrument_keys': test_instruments,
            'action': 'subscribe'
        }

        await nc.publish(constants.NATS_SUBJECT_INSTRUMENT_KEYS, json.dumps(subscription_data).encode())
        logger.info(f"Published test instrument subscription for bot {bot_id}")

        # Wait a bit for responses
        await asyncio.sleep(5)

        logger.info(f"Test completed. Received {tick_count} tick messages")

    except Exception as e:
        logger.error(f"Test failed: {e}")
    finally:
        await nc.close()

if __name__ == "__main__":
    asyncio.run(test_nats_communication())