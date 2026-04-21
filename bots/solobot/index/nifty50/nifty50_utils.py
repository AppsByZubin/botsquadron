#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
==================================================
 File:        nifty_utils.py
 Author:      Amit Mohanty
 
 Notes:
    - nifty utils to fetch premarket and intraday data.
    - apply sma strategy on intraday data.
==================================================
"""

from logger import create_logger
import csv
from pandas.tseries.offsets import BDay
from pandas import Timestamp
import pandas as pd
from urllib.parse import urlencode
from common import constants
import numpy as np
import pandas_ta as ta
from datetime import datetime, date, time, timedelta
import json
import math
import os
from pathlib import Path
import uuid
from zoneinfo import ZoneInfo
import calendar
from typing import Any, Dict, Optional, Set, Tuple

IST = ZoneInfo("Asia/Kolkata")
MARKET_CLOSE = time(15, 30)

logger = create_logger("NiftyUtilsLogger")

def premarket(upstox):
    """
    Args:
    - upstox: upstox client instance
            
    Notes:
    - fetch premarket data for nifty50 and return parsed data.
    """
    try:

        last_trading_day = valid_market_date(Timestamp.now() - BDay(1), upstox=upstox)
        logger.debug(f"Last trading day determined as: {last_trading_day.strftime('%Y-%m-%d')}")
        second_last_trading_day = valid_market_date(last_trading_day - BDay(1), upstox=upstox)
        logger.debug(f"Second last trading day determined as: {second_last_trading_day.strftime('%Y-%m-%d')}")

        nifty50_instrument_key = constants.NIFTY50_SYMBOL
        logger.debug(f"Fetching historical data for {nifty50_instrument_key} from {second_last_trading_day} to {last_trading_day}")

        response = upstox.get_historical_data(
            nifty50_instrument_key,
            second_last_trading_day,
            last_trading_day,
            "days",
            1
        )

        if response.status != constants.SUCCESS:
            logger.error(f"Failed to fetch historical data: {response.data}")
            raise Exception("Historical data fetch failed")
        
        # Parse the historical data JSON response
        candles = response.data.candles
        if not candles:
            logger.error("No candles found in the historical data response.")
            raise Exception("No historical data available") 
        
        parsed_data = [
            {
            "timestamp": candle[0],
            "open": candle[1],
            "high": candle[2],
            "low": candle[3],
            "close": candle[4]
            }
            for candle in candles
        ]

        logger.debug(f"Parsed historical data: {parsed_data}")
        return parsed_data
    
    except Exception as e:
        raise Exception(f"Failed to process premarket: {e}")


def get_nifty_historical_data_previous_day(upstox, instrument_key):
    """
    Args:
    - upstox: upstox client instance
    - instrument_key: Instrument key for Nifty 50
    - to_date: Date (YYYY-MM-DD)
            
    Notes:
    - fetch historical candles data for a given instrument key and date range.
    """
    try:
        last_trading_day = valid_market_date(Timestamp.now() - BDay(1), upstox=upstox)
        to_date = last_trading_day.strftime('%Y-%m-%d')

        historical_data = upstox.get_historical_data(
            instrument_key,
            to_date,
            to_date,
            "minutes",
            1
        )

        if historical_data.status != constants.SUCCESS:
            logger.error(f"Failed to fetch historical data: {historical_data.data}")
            raise Exception("Historical data fetch failed")
        
        return historical_data.data.candles
    
    except Exception as e:
        raise Exception(f"Failed to fetch historical data: {e}")

def get_instrument_intraday_data(upstox, instrument_key):
    """
    Args:
    - upstox: upstox client instance
    - instrument_key: Instrument key for Nifty 50
            
    Notes:
    - fetch intraday candles data from market open for a given instrument key.
    """
    try:

        # Calculate the time difference between 9:15 AM and the current time in minutes
        market_open_time = Timestamp.now().replace(hour=9, minute=15, second=0, microsecond=0)
        current_time = Timestamp.now()

        interval = int((current_time - market_open_time).total_seconds() / 60)
        intraday_data = upstox.get_intraday_data(
            instrument_key,
            "minutes",
            "1"
        )

        if intraday_data.status != constants.SUCCESS:
            logger.error(f"Failed to fetch historical data: {intraday_data.data}")
            raise Exception("intraday data fetch failed")
        
        return intraday_data.data.candles
    
    except Exception as e:
        raise Exception(f"Failed to fetch historical data from market open: {e}")


def get_option_contracts(upstox, symbol):
    """
    Args:
    - upstox: upstox client instance
    - symbol: Trading symbol (e.g., "NIFTY")
            
    Notes:
    - fetch all available option contracts for a given symbol.
    - save the data to a json file.
    """
    try:        
        data = {}
        response = upstox.get_option_contracts_by_instrument(symbol)
        if response.status != constants.SUCCESS:
            logger.error(f"Failed to fetch option contracts for {symbol}: {response.data}")
            raise Exception("Option contracts fetch failed")
        
        contracts = response.data
        for contract in contracts:
            expiry = contract.expiry.strftime("%Y-%m-%d")
            body = {
                "exchange": contract.exchange,
                "exchange_token": contract.exchange_token,
                "expiry": expiry,
                "freeze_quantity": contract.freeze_quantity,
                "instrument_key": contract.instrument_key,
                "instrument_type": contract.instrument_type,
                "lot_size": contract.lot_size,
                "minimum_lot": contract.minimum_lot,
                "name": contract.name,
                "segment": contract.segment,
                "strike_price": contract.strike_price,
                "tick_size": contract.tick_size,
                "trading_symbol": contract.trading_symbol,
                "underlying_key": contract.underlying_key,
                "underlying_symbol": contract.underlying_symbol,
                "underlying_type": contract.underlying_type,
                "weekly": contract.weekly
            }

            if expiry in data:
                data[expiry].append(body)
            else:
                data[expiry] = [body]

        if len(data) > 0:
            with open(constants.NIFTY50_OPTION_CONTRACTS_FILE, "w") as json_file:
                json.dump(data, json_file, indent=4)
            logger.info(f"Option contracts data saved for {symbol}")
    
    except Exception as e:
        raise Exception(f"Failed to fetch option contracts: {e}")


def _holiday_seed_file() -> Path:
    return Path(constants.DEFAULT_SOLOBOT_FILES_DIR) / "holiday_list.json"


def ensure_holiday_list_file(upstox=None) -> bool:
    holiday_path = Path(constants.HOLIDAY_LIST_FILE)
    if holiday_path.exists():
        return True

    holiday_path.parent.mkdir(parents=True, exist_ok=True)

    seed_file = _holiday_seed_file()
    if holiday_path != seed_file and seed_file.exists():
        try:
            with open(seed_file, "r", encoding="utf-8") as seed_handle:
                holidays = json.load(seed_handle)
            with open(holiday_path, "w", encoding="utf-8") as holiday_handle:
                json.dump(holidays, holiday_handle, indent=4)
            logger.info(f"Seeded holiday list from packaged file: {seed_file}")
            return True
        except Exception as exc:
            logger.warning(f"Failed to seed holiday list from {seed_file}: {exc}")

    if upstox is None:
        logger.warning(f"Market holiday file not found and no upstox client available: {holiday_path}")
        return False

    result = upstox.get_holday_list()
    logger.debug(f"Holiday status: {result.status}")
    if result.status != constants.SUCCESS:
        logger.error(f"Failed to fetch holiday list: {result.data}")
        return False

    holidays = []
    for h in result.data:
        body = {
            "date": h._date.strftime("%Y-%m-%d"),
            "description": h.description
        }
        holidays.append(body)

    with open(holiday_path, "w", encoding="utf-8") as holiday_handle:
        json.dump(holidays, holiday_handle, indent=4)
    logger.info(f"Holiday list saved to file: {holiday_path}")
    return True


def is_market_holiday(upstox, date):
    """
    Args:
    - date: Date to check (datetime object)
            
    Notes:
    - Check if the given date is a market holiday.
    """
    if not ensure_holiday_list_file(upstox=upstox):
        return False

    if is_date_present_in_holiday_file(date):
        return True

    return False


def is_date_present_in_holiday_file(date, upstox=None):
    """
    Args:
    - date: Date to check (datetime object)
            
    Notes:
    - Check if the given date is present in the holiday file.
    """
    if not ensure_holiday_list_file(upstox=upstox):
        return False

    try:
        with open(constants.HOLIDAY_LIST_FILE, "r", encoding="utf-8") as file:
            holidays = json.load(file)
    except FileNotFoundError:
        logger.warning(f"Market holiday file still not available: {constants.HOLIDAY_LIST_FILE}")
        return False
    except Exception as exc:
        logger.warning(f"Failed to read holiday list file {constants.HOLIDAY_LIST_FILE}: {exc}")
        return False

    date_str = date.strftime('%Y-%m-%d')
    # Check if the given date is in the holiday list
    for holiday in holidays:
        if holiday['date'] == date_str:
            logger.info(f"The date {date_str} is a market holiday for {holiday['description']}.")
            return True

    return False


def valid_market_date(check_date, upstox=None):
    """
    Args:
    - date: Date to check
            
    Notes:
    - Check if the given date is a valid market day (not a holiday).
    - User recursiopn to find the next valid market day.
    """
    is_holiday = is_date_present_in_holiday_file(check_date, upstox=upstox)

    # Normalize to date only
    if isinstance(check_date, datetime):
        check_date = check_date.date()

    if is_holiday:
        logger.debug(f"The date {check_date.strftime('%Y-%m-%d')} is a market holiday.")
        new_date = check_date - BDay(1)
        return valid_market_date(new_date, upstox=upstox)

    return  check_date


def _adjust_for_holiday_and_weekend(expiry: date) -> date:
    # If holiday or weekend, move backward day-by-day until a valid working day
    # (this matches your current intention; preserves expiry-advance behavior)
    while is_date_present_in_holiday_file(expiry) or expiry.weekday() >= 5:
        expiry -= timedelta(days=1)
    return expiry

def _compute_upcoming_weekly_expiry(dt: datetime) -> date:
    changeover = date(2025, 9, 1)

    # weekly expiry weekday based on changeover (Tue after 2025-09-01 else Thu)
    weekly_wd = 1 if dt.date() >= changeover else 3  # 1=Tue, 3=Thu

    wd = dt.weekday()
    days_ahead = (weekly_wd - wd) % 7
    if days_ahead == 0 and dt.time() >= MARKET_CLOSE:
        days_ahead = 7

    nominal_expiry = dt.date() + timedelta(days=days_ahead)
    nominal_expiry = _adjust_for_holiday_and_weekend(nominal_expiry)
    return nominal_expiry

def _compute_next_expiry_from(expiry: date, dt: datetime) -> date:
    """
    Given the upcoming expiry date (already adjusted), compute the next weekly expiry after that,
    then adjust for holiday/weekend.
    """
    changeover = date(2025, 9, 1)
    weekly_wd = 1 if dt.date() >= changeover else 3  # Tue/Thu anchor

    # Start searching from the day after the upcoming expiry.
    base = expiry + timedelta(days=1)

    wd = base.weekday()
    days_ahead = (weekly_wd - wd) % 7
    # If base is exactly the weekly weekday, this gives 0; we want next occurrence -> 0 is fine
    # because base is already "day after expiry", so 0 means that weekday is on base itself.
    next_expiry = base + timedelta(days=days_ahead)

    next_expiry = _adjust_for_holiday_and_weekend(next_expiry)
    return next_expiry

def _merge_contracts_for_expiries(options,contracts,next_expiry_key):
    next_expiry_contracts = options[next_expiry_key]

    for value in next_expiry_contracts:
        contracts.append(value)


def get_nifty_option_instruments(atm_price, trade_next_expiry=False):
    """
    Notes:
    - Returns weekly instruments.
    - If is_zero_dte=True AND today is expiry, select from the next-next expiry (one after upcoming).
    """
    if not os.path.exists(constants.NIFTY50_OPTION_CONTRACTS_FILE):
        raise Exception(f"Nifty 50 option instrument list not found.")

    with open(constants.NIFTY50_OPTION_CONTRACTS_FILE, "r") as file:
        options = json.load(file)

    configured_expiry = str(trade_next_expiry or "").strip()
    if not configured_expiry:
        raise ValueError("strategy-parameters.trade_expiry is required")

    if configured_expiry not in options:
        available_expiries = sorted(str(key) for key in options.keys())
        raise ValueError(
            f"Configured trade_expiry {configured_expiry!r} not found in "
            f"{constants.NIFTY50_OPTION_CONTRACTS_FILE}. Available expiries: {available_expiries}"
        )

    contracts = options[configured_expiry]
    
    selected_contracts = {}

    for contract in contracts:
        if (atm_price + 400) >= contract['strike_price'] and contract['strike_price'] >= (atm_price - 400):
            body = {
                "exchange": contract['exchange'],
                "expiry": contract['expiry'],
                "instrument_key": contract['instrument_key'],
                "instrument_type": contract['instrument_type'],
                "lot_size": contract['lot_size'],
                "minimum_lot": contract['minimum_lot'],
                "name": contract['name'],
                "segment": contract['segment'],
                "strike_price": contract['strike_price'],
                "trading_symbol": contract['trading_symbol'],
                "weekly": contract['weekly'],
            }

            if contract['strike_price'] not in selected_contracts:
                selected_contracts[contract['strike_price']] = [body]
            else:
                selected_contracts[contract['strike_price']].append(body)

    return selected_contracts

def get_spot_price(upstox,  symbol, instrument_key):
    """
    Args:
    - upstox: upstox client instance
    - symbol: Trading symbol (e.g., "NIFTY")
            
    Notes:
    - fetch the current ATM price for a given symbol.
    """
    try:        

        response = upstox.get_ltp(
            instrument_key
        )
        if hasattr(response, "to_dict"):
            resp = response.to_dict()
        else:
            resp = response.__dict__

        data = resp.get("data") or {}

        # Upstox commonly nests as: data -> { instrument_key: { "last_price": ... } }
        ik = instrument_key
        ltp = None

        if isinstance(data, dict):
            node = data.get(instrument_key.replace("|", ":"))
            if node is None:
                node = data.get(f'NSE_EQ:{symbol}')

            if node is None:
                logger.error(f"Instrument key {instrument_key} not found in response data keys={list(data.keys())}")
                raise Exception("Instrument key not found in response")

            # sometimes encoded
            if isinstance(node, dict):
                ltp = node.get("last_price") 

            if ltp is None:
                logger.error(f"LTP missing in response for {instrument_key}. Parsed data keys={list(data.keys()) if isinstance(data, dict) else type(data)}")
                raise Exception("LTP missing in response")

            return float(ltp)

    except Exception as e:
        raise Exception(f"Failed to fetch ATM price: {e}")


def save_ohlc_to_json(ohlc_data):
    """
    Args:
    - ohlc_data: Dictionary containing OHLC data
            
    Notes:
    - Saves OHLC data to a JSON file.
    """
    with open(constants.TREND_FILE, 'w') as json_file:
        json.dump(ohlc_data, json_file, indent=4)


def read_ohlc_from_json():
    """            
    Notes:
    - Reads OHLC data from a JSON file.
    """
    if not os.path.exists(constants.TREND_FILE):
        raise FileNotFoundError(f"The file {constants.TREND_FILE} does not exist.")
    with open(constants.TREND_FILE, 'r') as json_file:
        return json.load(json_file)


def get_previous_day_trend():
    """
    Notes:
    - Returns the previous day's trend and price range from stored OHLC data.
    """
    df = pd.DataFrame(read_ohlc_from_json())

    last_day = df.iloc[-2]
    before_last_day = df.iloc[-1]
    trend = ""
    price_range=[None] * 4

    last_day_close = last_day['close']
    change = last_day['close'] - last_day['open']
    range_ = last_day['high'] - last_day['low']

    if last_day['open'] > before_last_day['close'] and last_day['close'] > last_day['open']: 
        price_range[0] = last_day['open']
        price_range[1]  = before_last_day['close']
        price_range[2]  = last_day['close'] + (abs(change)/2)
        price_range[3] = last_day['close'] + abs(change)
        return constants.SUPER_BULLISH,price_range, last_day_close

    if last_day['open'] < before_last_day['close'] and last_day['close'] < last_day['open']: 
        price_range[0] = last_day['open']
        price_range[1] = before_last_day['close']
        price_range[2] = last_day['close'] - (abs(change)/2)
        price_range[3] = last_day['close'] - abs(change)
        return constants.SUPPER_BEARISH,price_range, last_day_close

    if change > 0 and change > 0.6 * range_:
        price_range[0] = last_day['open']
        price_range[1] = before_last_day['low']
        price_range[2] = last_day['close'] + (abs(change)/2)
        price_range[3] = last_day['close'] + abs(change)
        trend = constants.BULLISH
    elif change < 0 and abs(change) > 0.6 * range_:
        price_range[0] = last_day['close']
        price_range[1] = before_last_day['high']
        price_range[2] = last_day['close'] - (abs(change)/2)
        price_range[3] = last_day['close'] - abs(change)
        trend = constants.BEARISH
    else:
        price_range[0] = last_day['close'] - (abs(change)/2)
        price_range[1] = last_day['close'] - abs(change)
        price_range[2] = last_day['close'] + (abs(change)/2)
        price_range[3] = last_day['close'] + abs(change)
        trend = constants.SIDEWAYS
    
    return trend, price_range, last_day_close


def safe_float(value: Any) -> Optional[float]:
    try:
        f = float(value)
    except Exception:
        return None
    if not math.isfinite(f):
        return None
    return f


def read_previous_close_from_trend_file(trend_file: str = constants.TREND_FILE) -> Optional[float]:
    try:
        if not os.path.exists(trend_file):
            return None
        with open(trend_file, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        if not isinstance(data, list) or not data:
            return None
        if len(data) >= 2 and isinstance(data[-2], dict):
            return safe_float(data[-2].get("close"))
        if isinstance(data[-1], dict):
            return safe_float(data[-1].get("close"))
    except Exception:
        return None
    return None


def get_previous_close_for_gap(params: Optional[Dict[str, Any]]) -> Optional[float]:
    ht = (params.get("historical-trend") or {}) if isinstance(params, dict) else {}
    if isinstance(ht, dict):
        prev_close = (
            safe_float(ht.get("last-day-close"))
            or safe_float(ht.get("last_day_close"))
            or safe_float(ht.get("previous-close"))
            or safe_float(ht.get("previous_close"))
        )
        if prev_close is not None and prev_close > 0:
            return prev_close

    prev_close = read_previous_close_from_trend_file(constants.TREND_FILE)
    return prev_close if (prev_close is not None and prev_close > 0) else None


def calculate_gap_percent(previous_close: Any, today_open: Any, precision: int = 4) -> Optional[float]:
    prev = safe_float(previous_close)
    opn = safe_float(today_open)
    if prev is None or opn is None or prev <= 0 or opn <= 0:
        return None
    gap_pct = abs(((opn - prev) / prev) * 100.0)
    return float(round(gap_pct, int(precision)))


def classify_gap_direction(
    gap_pct: Optional[float] = None,
    previous_close: Any = None,
    today_open: Any = None,
) -> Optional[str]:
    prev = safe_float(previous_close)
    opn = safe_float(today_open)
    if prev is not None and opn is not None and prev > 0 and opn > 0:
        if opn > prev:
            return constants.GAP_UP
        if opn < prev:
            return constants.GAP_DOWN
        return constants.FLAT

    if gap_pct is None:
        return None
    if gap_pct > 0:
        return constants.GAP_UP
    if gap_pct < 0:
        return constants.GAP_DOWN
    return constants.FLAT
    
    
def should_place_order(tick_data):
    """
    Args:
    - tick_data: tick data containing 'ltp', 'sma_fast', and 'sma_slow' values.
            
    Notes:
    - get SELL or BUY sugnal based on tick data.
    """
    if  tick_data['ltp'] > tick_data['sma_fast'] > tick_data['sma_slow']:
        return 'BUY'
    elif  tick_data['ltp'] < tick_data['sma_fast'] < tick_data['sma_slow']:
        return 'SELL'
    return None


def log_order(order_id, signal, price, qty):
    """
    Args:
    - signal: Buy or Sell signal
    - price: current ltp price
            
    Notes:
    - save the Order Placed log to a file for future analysis.
    """

    fieldnames = ['OrderId', 'Time', 'Signal', 'Price', 'Qty', 'Amount']

    if order_id == "":
        order_id = uuid.uuid1()
    
    current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    data = {
        'OrderId': order_id,
        'Time': current_datetime,
        'Signal': signal,
        'Price': price,
        'Qty': qty,
        'Amount': price * qty
    }

    # Create a Path object
    file_path = Path(constants.ORDER_LOG)

    # Check if the file exists and is not empty
    file_exists = file_path.exists() and file_path.stat().st_size > 0

    # Open the file in append mode ('a') which will create it if not present
    with open(file_path, 'a', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)

        # Write the header only if the file didn't exist or was empty
        if not file_exists:
            writer.writeheader()

        # Write the data row
        writer.writerow(data)
        logger.info(f"{current_datetime} Order:{signal} at {price} with Qty: {qty} Amount: {price*qty}")
 

"""
Will modify the angle depending on performance to angle based.
https://chatgpt.com/share/68efe322-3218-8007-ad9c-d2a283841f29

"""

def detect_crossover_and_signal(df, min_angle=0.01):
    """
    Args:
        df: DataFrame with columns ['close', 'sma_fast', 'sma_slow']
        min_angle: minimum normalized slope difference to confirm crossover (default: 0.02%)
        
    Returns:
        dict: {
            'signal': 'BUY' | 'SELL' | None,
            'crossover_factor': float,
            'last_price': float
        }
    """
    if df.shape[0] < 5:
        return {'signal': None}

    # Use last few candles for smoother slope estimation
    fast_recent = df['sma_fast'].iloc[-3:]
    slow_recent = df['sma_slow'].iloc[-3:]

    prev_fast, prev_slow = df['sma_fast'].iloc[-2], df['sma_slow'].iloc[-2]
    curr_fast, curr_slow = df['sma_fast'].iloc[-1], df['sma_slow'].iloc[-1]
    last_price = df['close'].iloc[-1]

    # --- Step 1: Detect crossover condition
    crossed_up = (prev_fast <= prev_slow) and (curr_fast > curr_slow)
    crossed_down = (prev_fast >= prev_slow) and (curr_fast < curr_slow)
    logger.debug(f"Crossed Up: {crossed_up}, Crossed Down: {crossed_down}")
    # --- Step 2: Compute normalized slope angle
    slope_fast = (fast_recent.iloc[-2] - fast_recent.iloc[0]) / fast_recent.iloc[0]
    slope_slow = (slow_recent.iloc[-2] - slow_recent.iloc[0]) / slow_recent.iloc[0]
    crossover_strength = abs(slope_fast - slope_slow)

    # --- Step 3: Normalize by price to get true % change
    crossover_factor = round(crossover_strength * 100, 4)  # percentage difference
    logger.debug(f"Crossover Factor: {crossover_factor}%, crossover_strength: {crossover_strength} Slope Fast: {slope_fast}, Slope Slow: {slope_slow}")
    # --- Step 4: Filter noise
    if crossover_strength < min_angle:
        return {'signal': None}

    # --- Step 5: Confirm direction with slope momentum
    if crossed_up and slope_fast > 0:
        return {'signal': 'BUY', 'crossover_factor': crossover_factor, 'last_price': last_price}

    elif crossed_down and slope_fast < 0:
        return {'signal': 'SELL', 'crossover_factor': crossover_factor, 'last_price': last_price}

    else:
        return {'signal': None}
