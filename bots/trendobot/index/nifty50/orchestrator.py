#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
==================================================
 File:        orchestrator.py
 Author:      Amit Mohanty
 
 Notes:
    - checks if current day is weekend or not.
    - stores last 2days ohlc data in trend.json.
    - wait for market to open if current time is before 9:15 AM.
    - if open after 9:15 AM fetch intraday data from market open.
==================================================
"""

import os
import sys
import common.constants as constants

from utils.mock_order_utils import MockOrderSystem
from broker.upstox_helper import UpstoxHelper
from datetime import datetime, time,date, timezone
from zoneinfo import ZoneInfo
import time as t
import pandas as pd
from logger import create_logger
import upstox_client
import pandas_ta as ta


# Storage
candles = []
current_candle = None
future_contract_candle= None
current_minute = None
future_contract_current_minute = None

from index.nifty50.nifty_utils import (
    premarket,
    get_instrument_intraday_data,
    is_market_holiday,
    get_option_contracts,
    get_nifty_historical_data_previous_day,
    get_nifty_option_instruments
)

from utils.generic_utils  import (
    save_ohlc_to_json,
    get_previous_day_trend,
)

from index.nifty50.strategy_utils import (
    sma_crossover_strategy,
    ema_angle_rsi_strategy
)

ist = ZoneInfo("Asia/Kolkata")
logger = create_logger("OrchestratorLogger")

#prices =  deque(maxlen=100) 
prices = []
minutes_processed = {}
future_minutes_processed = {}
minutes_candles_sticks = []
present_atm_price =0
infocus_contracts = {}

def handle_tick_message(message,selected_contracts, prev_trend, df_nifty, df_future, strategy, mock_order=None):
    """
    Build 1-minute OHLC candles from Upstox live 'ltpc' ticks and append to df_nifty, df_future.
    
    Args:
        message (dict): Upstox websocket message (already parsed JSON)
        prev_trend: previous day's trend (unused here)
        df_nifty (pd.DataFrame): existing DataFrame to append new candles into
    
    Returns:
        pd.DataFrame: updated DataFrame with all completed candles
    """
    global current_candle, current_minute, candles, present_atm_price, infocus_contracts
    future_instrument_key = None
    ltt_dt = None
    future_ltt_dt = None

    if "feeds" not in message:
        return
    
    feed = message["feeds"]
    
    for key, data in feed.items():
        if present_atm_price > 0 or present_atm_price in infocus_contracts:
            contracts = selected_contracts.get(present_atm_price)
            if contracts:
                for contract in contracts:
                    if contract['instrument_type'] == "CE":
                        instrument_key = contract['instrument_key']
                        if key == instrument_key:
                            ltpc = data.get("ltpc", {})
                            infocus_contracts[str(present_atm_price)+'-CE'] = {'symbol': instrument_key, 'ltp': ltpc.get("ltp", 0)}
                    elif contract['instrument_type'] == "PE":
                        instrument_key = contract['instrument_key']
                        if key == instrument_key:
                            ltpc = data.get("ltpc", {})
                            infocus_contracts[str(present_atm_price)+'-PE'] = {'symbol': instrument_key, 'ltp': ltpc.get("ltp", 0)}
            
        if key == constants.NIFTY50_SYMBOL:
            ltpc = data.get("ltpc", {})
            if not ltpc:
                continue

            # Extract LTP and timestamp
            ltp = float(ltpc["ltp"])
            ltt = ltpc["ltt"]
            present_atm_price = int(round(ltp/ 50) * 50)

            ltt_dt = datetime.fromtimestamp(float(ltt) / 1000, ist).strftime('%Y-%m-%d %H:%M')
            candle_minute = ltt_dt
            
            # --- New 1-minute candle begins ---
            if current_minute != candle_minute:
                
                if current_candle:
                    # Close old candle and append it to DataFrame
                    candles.append(current_candle)
                    new_row = pd.DataFrame([current_candle])
                    df_nifty = pd.concat([df_nifty, new_row], ignore_index=True)
                    logger.debug(f"✅ Closed candle at {current_candle['timestamp']} -> {current_candle}")

                # Initialize new candle
                current_candle = {
                    "timestamp": candle_minute,
                    "open": ltp,
                    "high": ltp,
                    "low": ltp,
                    "close": ltp
                }
                current_minute = candle_minute

            # --- Update running candle ---
            else:
                current_candle["high"] = max(current_candle["high"], ltp)
                current_candle["low"] = min(current_candle["low"], ltp)
                current_candle["close"] = ltp

        # Handle Nifty Future Contract
        future_contract = selected_contracts.get("Nifty_Future")
        if future_contract is not None:
            future_instrument_key = future_contract['instrument_key']
            if key == future_instrument_key:
                future_ltpc = data.get("ltpc", {})
                if not future_ltpc:
                    continue

                # Extract LTP and timestamp
                future_ltp = float(future_ltpc["ltp"])
                future_ltt = future_ltpc["ltt"]

                future_ltt_dt = datetime.fromtimestamp(float(future_ltt) / 1000, ist).strftime('%Y-%m-%d %H:%M')
                future_contract_minute = future_ltt_dt
                
                if future_contract_current_minute != future_contract_minute:
                    if future_contract_candle:
                        # Close old candle and append it to DataFrame
                        new_row = pd.DataFrame([future_contract_candle])
                        df_future = pd.concat([df_future, new_row], ignore_index=True)
                        logger.debug(f"✅ Closed future candle at {future_contract_candle['timestamp']} -> {future_contract_candle}")

                    # Initialize new candle
                    future_contract_candle = {
                        "timestamp": future_contract_minute,
                        "open": future_ltp,
                        "high": future_ltp,
                        "low": future_ltp,
                        "close": future_ltp
                    }
                    future_contract_current_minute = future_contract_minute
                else:
                    future_contract_candle["high"] = max(future_contract_candle["high"], future_ltp)
                    future_contract_candle["low"] = min(future_contract_candle["low"], future_ltp)
                    future_contract_candle["close"] = future_ltp    
            
        if ltt_dt and not ltt_dt in minutes_processed and future_ltt_dt and not future_ltt_dt in future_minutes_processed:
            minutes_processed[ltt_dt] = True
            future_minutes_processed[future_ltt_dt] = True
            if strategy == constants.SMA_CROSSOVER:
                sma_crossover_strategy(feed,df_nifty,ltt_dt,mock_order)
            elif strategy == constants.RSI_EMA_ANGLE:
                ema_angle_rsi_strategy(infocus_contracts,df_nifty,ltt_dt,mock_order)
            else:
                logger.error(f"Failed to find strategy {strategy}")
                raise Exception(f"Failed to find strategy {strategy}")



def subscribe_live_ticks(selected_contracts, prev_trend, token, df_nifty, df_future,  strategy, mock):
    """
    Args:
    - prev_trend: Previous day trend (e.g., "uptrend" or "downtrend")
    - token: Upstox API access token
            
    Notes:
    - Subscribe to live market data ticks using Upstox WebSocket API.
    """
    mock_order =None
    if mock == True:
        mock_order = MockOrderSystem(tsl_buffer=3)

    logger.info(f"Simulation initiated: {mock}")

    list_of_instruments = []
    if len(selected_contracts) > 0:
        key_list = list(selected_contracts.keys())

        for key in key_list:
            contracts = selected_contracts[key]
            for contract in contracts:
                list_of_instruments.append(contract['instrument_key'])

    list_of_instruments.append(constants.NIFTY50_SYMBOL)
    logger.info(f"Tracking instruments: {list_of_instruments}")  
    configuration = upstox_client.Configuration()
    configuration.access_token = token

    streamer = upstox_client.MarketDataStreamerV3(
        upstox_client.ApiClient(configuration), list_of_instruments, "full")
    
    def on_message(message):
        handle_tick_message(message,selected_contracts, prev_trend, df_nifty, df_future, strategy, mock_order)
  
    def on_error(error):
        logger.warning("WebSocket Streamimg error:", error)

    def on_close():
        logger.info("WebSocket Streamimg closed")

    def on_open():
        logger.info("WebSocket Streamimg opened")
    
    streamer.on("message", on_message)
    streamer.on("error", on_error)
    streamer.on("close", on_close)
    streamer.on("open", on_open)
    streamer.connect()


def orchestrator(instruments, strategy, mock):
    """
    Args:
    - instruments: Comma-separated string of instrument symbols to track (e.g., "nifty50")
    - strategy: strategy selected
    - mock: true/false for mock trading
            
    Notes:
    - Main orchestrator function to manage the trading workflow.
    - premarket activities
    """
    logger.info(f"Orchestrating with instruments: {instruments}")
    try:
        selected_contracts = {}
        # Check if today is a weekend
        today = datetime.today().weekday()
        if today >= 5:  # 5 = Saturday, 6 = Sunday
            logger.warning("Today is a weekend no trading for today. Exiting orchestrator.")
            sys.exit(constants.SUCCESS_CODE)

        # Initialize Upstox client
        apiAccessToken = os.getenv(constants.UPSTOX_API_ACCESS_TOKEN)
        
        if apiAccessToken is None:
            logger.error("Upstox credentials not set in envoirnment.")
            sys.exit(constants.FAIL_CODE)

        upstox = UpstoxHelper(apiAccessToken)
        if upstox is None:
            logger.error("Upstox client not created.")
            sys.exit(constants.FAIL_CODE)

        logger.info("Upstox client created successfully.")
        
        # Check if today is a market holiday
        if is_market_holiday(upstox, date.today()):
            logger.info("No trading today, Take the day off. Exiting orchestrator.")
            return

        # Fetch and store option contracts for NIFTY50
        get_option_contracts(upstox, constants.NIFTY50_SYMBOL)
        
        # Fetch premarket data for the last 2 days
        ohlc_two_days = premarket(upstox)
        if ohlc_two_days is None:
            logger.error("Failed to fetch premarket data.")
            sys.exit(constants.FAIL_CODE)   
        
        # Save OHLC data to JSON
        save_ohlc_to_json(ohlc_two_days)

        logger.info("Premarket data saved successfully.")
        
        # Determine previous day trend and price range
        trend, price_range = get_previous_day_trend()
        logger.info(f"Previous day trend: {trend}")
        logger.info(f"Price range: Support- {price_range[0]}, Deep Support- {price_range[1]}, Resistance- {price_range[2]}, Deep Resistance- {price_range[3]}")

        # Fetch previous day's intraday 1-minute candles
        previous_day_1min_candles = get_nifty_historical_data_previous_day(upstox, constants.NIFTY50_SYMBOL)
        if previous_day_1min_candles is None:
            logger.error("Failed to fetch previous day intraday data.")
            sys.exit(constants.FAIL_CODE)
        previous_day_1min_candles.reverse()

        # Fetch upcoming NIFTY future contract
        future_contract = upstox.get_future_contracts_by_instrument(month_offset=0)
        if future_contract is None:
            logger.error("Failed to fetch upcoming NIFTY future contract.")
            sys.exit(constants.FAIL_CODE)
        logger.info(f"Upcoming NIFTY future key {future_contract['instrument_key']} contract: {future_contract['trading_symbol']} expiring on {future_contract['expiry']}")
        selected_contracts["Nifty_Future"]=future_contract

        # Fetch previous day's intraday candles for NIFTY future contract
        previous_day_future_candles = get_nifty_historical_data_previous_day(upstox, future_contract['instrument_key'])
        if previous_day_future_candles is None:
            logger.error("Failed to fetch previous day intraday data.")
            sys.exit(constants.FAIL_CODE)
        previous_day_future_candles.reverse()
        logger.info(f"Fetched previous day intraday data successfully. length: {len(previous_day_future_candles)} candles.")

        # Wait for market to open if before 9:15 AM
        current_time = datetime.now(ist).time()
        market_start_time = time(9, 15)
        market_end_time = time(22, 30)
        
        if current_time > market_end_time:
            logger.info("Market already closed for the day. Exiting orchestrator.")
            sys.exit(constants.SUCCESS_CODE)
        elif current_time < market_start_time:
            wait_time = (datetime.combine(datetime.today(), market_start_time) - datetime.now()).seconds - 2
 
            while wait_time > 0:
                logger.debug(f"Waiting for market to open. {wait_time} seconds remaining.")
                t.sleep(min(wait_time, 10))  # Sleep for 10 seconds or remaining time
                wait_time -= 10
            logger.info("Market got open now.")

        else:
            logger.info("Market is already open.")
            # Fetch intraday data from market open
            data = get_instrument_intraday_data(upstox, constants.NIFTY50_SYMBOL)
            data.reverse()
            previous_day_1min_candles.extend(data)
            # Fetch future contract intraday data
            future_data = get_instrument_intraday_data(upstox, future_contract['instrument_key'])
            future_data.reverse()
            previous_day_future_candles.extend(future_data)

        # Determine ATM option contracts based on last close price
        if previous_day_1min_candles is not None and len(previous_day_1min_candles) > 0:
            lastest_1min_candle = previous_day_1min_candles[len(previous_day_1min_candles)-1]
            close_price = lastest_1min_candle[4]
            atm_price = int(round(close_price/ 50) * 50)
            selected_contracts=get_nifty_option_instruments(atm_price)

        # Prepare DataFrame from previous day's candles
        columns = ["timestamp", "open", "high", "low", "close", "volume", "OI"]
        df_nifty = pd.DataFrame(previous_day_1min_candles, columns=columns)
        columns_to_drop = ["volume", "OI"]
        df_nifty.drop(columns=columns_to_drop, axis=1, inplace=True)
 
        last_timestamp = df_nifty['timestamp'].iloc[-1]
        dt = datetime.fromisoformat(last_timestamp)
        dt_ist = dt.astimezone(ist)
        formatted = dt_ist.strftime("%Y-%m-%d %H:%M")
        minutes_processed[formatted] = True

        # Prepare DataFrame from previous day's future contract candles
        df_future = pd.DataFrame(previous_day_future_candles, columns=columns)
        df_future.drop(columns=columns, axis=1, inplace=True)

        future_last_timestamp = df_future['timestamp'].iloc[-1]
        future_dt = datetime.fromisoformat(future_last_timestamp)
        future_dt_ist = future_dt.astimezone(ist)
        future_formatted = future_dt_ist.strftime("%Y-%m-%d %H:%M")
        future_minutes_processed[future_formatted] = True   

        subscribe_live_ticks(selected_contracts,trend, apiAccessToken, df_nifty, df_future, strategy, mock)
    except Exception as e:
        logger.error(f"An error occurred in orchestrator: {e}")
        sys.exit(constants.FAIL_CODE)


