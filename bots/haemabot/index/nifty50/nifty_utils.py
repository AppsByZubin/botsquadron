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
from pandas.tseries.offsets import BDay
from pandas import Timestamp
from urllib.parse import urlencode
import common.constants as constants
from datetime import datetime, date, time, timedelta
import json
import os
from zoneinfo import ZoneInfo
import calendar
from typing import Optional, Set, Tuple

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

        last_trading_day = valid_market_date(Timestamp.now() - BDay(1))
        logger.debug(f"Last trading day determined as: {last_trading_day.strftime('%Y-%m-%d')}")
        second_last_trading_day = valid_market_date(last_trading_day - BDay(1))
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
        last_trading_day = valid_market_date(Timestamp.now() - BDay(1))
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


def is_market_holiday(upstox, date):
    """
    Args:
    - date: Date to check (datetime object)
            
    Notes:
    - Check if the given date is a market holiday.
    """

    # Check if the holiday file exists
    if not os.path.exists(constants.HOLIDAY_LIST_FILE):
        logger.warning(f"Market holiday file not found: {constants.HOLIDAY_LIST_FILE}")
        result = upstox.get_holday_list()
        logger.debug(f"Holiday status: {result.status}")
        if result.status != constants.SUCCESS:
            logger.error(f"Failed to fetch holiday list: {result.data}")
            raise Exception("Holiday list fetch failed")
        
        holidays = []
        for h in result.data:
            body = {
                "date": h._date.strftime("%Y-%m-%d"),
                "description": h.description
            }
            holidays.append(body)

        with open(constants.HOLIDAY_LIST_FILE, "w") as json_file:
            json.dump(holidays, json_file, indent=4)
        logger.debug(f"Saving holiday list to file. Total holidays: {len(holidays)}")


    if is_date_present_in_holiday_file(date):
        return True

    return False


def is_date_present_in_holiday_file(date):
    """
    Args:
    - date: Date to check (datetime object)
            
    Notes:
    - Check if the given date is present in the holiday file.
    """

    # Check if the holiday file exists
    # Load the holiday file
    with open(constants.HOLIDAY_LIST_FILE, "r") as file:
        holidays = json.load(file)

    date_str = date.strftime('%Y-%m-%d')
    # Check if the given date is in the holiday list
    for holiday in holidays:
        if holiday['date'] == date_str:
            logger.info(f"The date {date_str} is a market holiday for {holiday['description']}.")
            return True

    return False


def valid_market_date(check_date):
    """
    Args:
    - date: Date to check
            
    Notes:
    - Check if the given date is a valid market day (not a holiday).
    - User recursiopn to find the next valid market day.
    """

    is_holiday = is_date_present_in_holiday_file(check_date)

    # Normalize to date only
    if isinstance(check_date, datetime):
        check_date = check_date.date()

    if is_holiday:
        logger.debug(f"The date {check_date.strftime('%Y-%m-%d')} is a market holiday.")
        new_date = check_date - BDay(1)
        return valid_market_date(new_date)

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

    dt = datetime.now(IST)

    # upcoming expiry (as per your existing logic)
    nominal_expiry = trade_next_expiry
    contracts = options[nominal_expiry]
    
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
