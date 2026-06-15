#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
==================================================
 File:        generic_utils.py
 Author:      Amit Mohanty
 
 Notes:
    - ohlc data read/write to json file.
    - get previous day trend and price range.
    - get buy/sell signal based on tick data.
    - log order placed to a file.
==================================================
"""

import json
import os
import math
import pandas as pd
import common.constants as constants
from logger import create_logger
from datetime import datetime
import csv
from pathlib import Path
import uuid
from typing import Any, Dict, Optional

logger = create_logger("GenericUtilsLogger")

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
    ht = (params.get("input-data") or {}) if isinstance(params, dict) else {}
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
