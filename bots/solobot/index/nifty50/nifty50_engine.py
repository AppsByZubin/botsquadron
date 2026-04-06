import sys
import os
import time as t
import asyncio
import json
from datetime import datetime, time
from zoneinfo import ZoneInfo
import nats

from common import constants
from broker.upstox_helper import UpstoxHelper
from logger import create_logger

from index.nifty50.nifty50_utils import (
    premarket,
    get_instrument_intraday_data,
    get_option_contracts,
    get_nifty_option_instruments,
    get_spot_price,
    save_ohlc_to_json,
    get_previous_day_trend
)

ist = ZoneInfo("Asia/Kolkata")
logger = create_logger("Nifty50EngineLogger")

async def nifty50_engine(strategy, mode, param_data):
    logger.info(f"Starting Nifty50 Engine with strategy: {strategy}, mode: {mode}")
    
    # Connect to NATS
    nc = await nats.connect(constants.NATS_URL)
    logger.info("Connected to NATS")

    try:
        # Implement the logic to fetch data, analyze, and execute trades based on the strategy and

        api_token = os.getenv(constants.UPSTOX_API_ACCESS_TOKEN)
        if not api_token:
            logger.error("API token not found. Please set UPSTOX_API_ACCESS_TOKEN.")
            return

        upstox = UpstoxHelper(api_token, is_sandbox=False)
        if upstox is None:
            logger.error("Upstox client not created.")
            sys.exit(constants.FAIL_CODE)

        get_option_contracts(upstox, constants.NIFTY50_SYMBOL)

        ohlc_two_days = premarket(upstox)
        if ohlc_two_days is None:
            logger.error("Failed to fetch premarket data.")
            sys.exit(constants.FAIL_CODE)

        last_day_close=0

        if len(ohlc_two_days) ==2:
            save_ohlc_to_json(ohlc_two_days)

            trend, price_range, last_day_close = get_previous_day_trend()
            logger.info(f"Previous day trend: {trend}")
            logger.info(
                f"Price range: Support-{price_range[0]}, Deep Support-{price_range[1]}, "
                f"Resistance-{price_range[2]}, Deep Resistance-{price_range[3]}"
            )

        future_contract = upstox.get_future_contracts_by_instrument(month_offset=0)
        if future_contract is None:
            logger.error("Failed to fetch upcoming NIFTY future contract.")
            sys.exit(constants.FAIL_CODE)
 
        # expose previous-day close to strategy for gap% calculations
        if param_data and isinstance(param_data, dict):
            ht_cfg = param_data.get("historical-trend")
            if not isinstance(ht_cfg, dict):
                ht_cfg = {}
                param_data["historical-trend"] = ht_cfg
            ht_cfg["last-day-close"] = float(last_day_close)

        selected_contracts = None			 
        intraday_day_1min_candles = []
        intraday_day_future_candles = []
        minutes_processed = {}
        future_minutes_processed = {}

        now_ist = datetime.now(ist)
        current_time = now_ist.time()

        market_start_time = time(9, 15)
        market_end_time = time(15, 30)

        if current_time > market_end_time:
            logger.info("Market already closed for the day. Exiting.")
            sys.exit(constants.SUCCESS_CODE)
        
        elif current_time < market_start_time:
																		  
            market_start_dt = datetime.combine(now_ist.date(), market_start_time, tzinfo=ist)					  
            wait_time = int((market_start_dt - now_ist).total_seconds()) - 2

            while wait_time > 0:
                logger.debug(f"Waiting for market to open... {wait_time}s remaining")
                await asyncio.sleep(min(wait_time, 10))
                wait_time -= 10

            spot_price = get_spot_price(upstox, constants.NIFTY50, constants.NIFTY50_SYMBOL)
            if spot_price is None:
                logger.error("Failed to fetch nifty 50 spot price at market open.")
                sys.exit(constants.FAIL_CODE)
            
            selected_contracts = get_nifty_option_instruments(spot_price, "2026-04-07")

            logger.info(f"Market open now. with spot price: {spot_price}. Bootstrapping intraday candles from market open.")

        else:																		  
            logger.info("Market already open. Bootstrapping intraday candles.")
            data = get_instrument_intraday_data(upstox, constants.NIFTY50_SYMBOL)
            data.reverse()
            intraday_day_1min_candles.extend(data)


        # Extract instrument keys from selected contracts
        instrument_keys = []
        for strike_price, contracts in selected_contracts.items():
            for contract in contracts:
                instrument_keys.append(contract['instrument_key'])

        # Send instrument keys to marketfeeder via NATS
        bot_id = f"nifty50_{strategy}_{mode}_{int(t.time())}"
        instrument_data = {
            'bot_id': bot_id,
            'instrument_keys': instrument_keys,
            'action': 'subscribe'
            }
            
        await nc.publish(constants.NATS_SUBJECT_INSTRUMENT_KEYS, json.dumps(instrument_data).encode())
        logger.info(f"Published instrument keys to marketfeeder for bot {bot_id}: {len(instrument_keys)} instruments: {instrument_keys}")

        # Subscribe to tick data
        async def tick_data_handler(msg):
            try:
                tick_data = json.loads(msg.data.decode())
                logger.debug(f"Received tick data: {tick_data}")
                    
                # Check if this tick data is for our instruments
                instrument_key = tick_data.get('instrument_key')
                if instrument_key in instrument_keys:
                    # Process tick data for trading logic
                    price = tick_data.get('price', 0)
                    volume = tick_data.get('volume', 0)
                    timestamp = tick_data.get('timestamp')
                        
                    logger.info(f"Processing tick for {instrument_key}: price={price}, volume={volume}")
                        
                    # TODO: Implement trading logic based on tick data
                    # This is where you would implement your strategy logic
                        
            except json.JSONDecodeError as e:
                logger.error(f"Failed to decode tick data: {e}")
            except Exception as e:
                logger.error(f"Error processing tick data: {e}")

            sub = await nc.subscribe(constants.NATS_SUBJECT_TICK_DATA, cb=tick_data_handler)
            logger.info("Subscribed to tick data from marketfeeder")

            # Keep the connection alive and process messages
            while True:
                await asyncio.sleep(1)
                
    except Exception as e:
        logger.error(f"Error in nifty50_engine: {e}")
    finally:
        await nc.close()
