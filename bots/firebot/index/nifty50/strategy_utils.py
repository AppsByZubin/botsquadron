#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
==================================================
 File:        feeder_utils.py
 Author:      Amit Mohanty
 
 Notes:
    - feeder utils to select strategy
==================================================
"""

from logger import create_logger
import common.constants as constants
import pandas_ta as ta
import numpy as np
import talib

from utils.generic_utils  import (
    log_order,
    detect_crossover_and_signal
)

logger = create_logger("StrategyUtilsLogger")

def sma_crossover_strategy(feed, df, minutes_processed, ltt_dt, mock_order=None):
    """
    Args:
        message (dict): Upstox websocket message (already parsed JSON)
        df (pd.DataFrame): existing DataFrame to append new candles into
        minutes_processed: key-value pair to check if the datetime present in df
        ltt: last traded price in data stream 
            
    Notes:
    - Apply sma crossover strategy using candles and greeks.
    """

    logger.info(f"Processing new minute: {ltt_dt}")

    df['sma_fast'] = ta.sma(df['close'], length=constants.SMA_SHORT)
    df['sma_slow'] = ta.sma(df['close'], length=constants.SMA_LONG)

    response = detect_crossover_and_signal(df)
    logger.debug(f"Crossover detection response: {response}")
    if response and response['signal'] != None:
        qty = 0
        if response['crossover_factor'] >= 250 and response['crossover_factor'] <= 400:
            qty = 1
        elif response['crossover_factor'] > 400 and response['crossover_factor'] <= 600:
            qty = 2
        elif response['crossover_factor'] > 600:
            qty = 3

        if qty > 0:
            if mock_order:
                mock_order.place_order("NIFTY50", "BUY", 50, price=100, target=120, stoploss=95)

                logger.info(f"Signal: {response['signal']} at Price: {response['last_price']} with Qty: {qty}.")



    # calculate greeks from here


def ema_angle_rsi_strategy(infocus_contracts, df, ltt_dt, mock_order=None):
    """
    Args:
        message (dict): Upstox websocket message (already parsed JSON)
        df (pd.DataFrame): existing DataFrame to append new candles into
        minutes_processed: key-value pair to check if the datetime present in df
        ltt: last traded price in data stream 
            
    Notes:
    - Apply ema angle rsi strategy using candles and greeks.
    """

    logger.info(f"Processing new minute: {ltt_dt}")
    
    short_window = 9
    long_window = 15
    price = df['close'].iloc[-1]
    df["EMA_fast"] = ta.ema(df['close'],length=short_window)
    df["EMA_slow"] = ta.ema(df['close'],length=long_window)

    # Compute indicators
    df['RSI_7'] = talib.RSI(df['close'], timeperiod=7)
    df['RSI_SMA_14'] = ta.sma(df['RSI_7'], length=14)
    #df['ATR_14'] = talib.ATR(df['high'], df['low'], df['close'], timeperiod=14)

    # --- Compute Slope (Normalized Difference) ---
    window = 3  # same as your fast_recent length
    df["Slope_fast"] = (df['EMA_fast'] - df['EMA_fast'].shift(window)) / window
    df["Slope_slow"] = (df['EMA_slow'] - df['EMA_slow'].shift(window)) / window
    df["Slope_RSI_7"] = (df['RSI_7'] - df['RSI_7'].shift(1)) / 1

    # --- Compute Angle in Degrees ---
    df["Angle_fast"] = np.degrees(np.arctan(df["Slope_fast"]))
    df["Angle_slow"] = np.degrees(np.arctan(df["Slope_slow"]))
    df["Angle_RSI_7"] = np.degrees(np.arctan(df["Slope_RSI_7"]))

    ema_fast = df["EMA_fast"].iloc[-1]
    ema_slow = df["EMA_slow"].iloc[-1]
    angle_fast = df["Angle_fast"].iloc[-1]

    if mock_order:
        #mock_order.check_targets("NIFTY50", price)
        atm_price = int(round(price/ 50) * 50)
        for _, contract_data in infocus_contracts.items():
            mock_order.check_targets(contract_data['symbol'], contract_data['ltp'])

        order = mock_order.get_open_order()
        if  order is None:
            
            if (ema_fast > ema_slow) and (df["RSI_7"].iloc[-3] < 50 < df["RSI_7"].iloc[-1]) and (angle_fast > 25):
                description = f"Price: {price}, Type: CE, ATM: {atm_price}, LTP: {contract_data['ltp']}, Total Price: {contract_data['ltp']*75}"

                contract_data=infocus_contracts[str(atm_price)+'-CE']
                target = contract_data['ltp'] + (0.3 * contract_data['ltp'])
                stoploss = contract_data['ltp'] - (0.1 * contract_data['ltp'])
                # target = (df['close'].iloc[-1]) + (2 * df['ATR_14'].iloc[-1])
                # stoploss = (df['close'].iloc[-2]) - (1.2 * df['ATR_14'].iloc[-1])
                mock_order.place_order(contract_data['symbol'], "BUY", 75, price, atm_price, description, target, stoploss)

            elif (ema_fast < ema_slow) and (df["RSI_7"].iloc[-3] > 50 > df["RSI_7"].iloc[-1]) and (angle_fast > -25):
                description = f"Price: {price}, Type: PE, ATM: {atm_price}, LTP: {contract_data['ltp']}, Total Price: {contract_data['ltp']*75}"

                contract_data=infocus_contracts[str(atm_price)+'-PE']
                target = contract_data['ltp'] + (0.3 * contract_data['ltp'])
                stoploss = contract_data['ltp'] - (0.1 * contract_data['ltp'])  
                # target = (df['close'].iloc[-1]) - (2 * df['ATR_14'].iloc[-1])
                # stoploss = (df['close'].iloc[-2]) + (1.2 * df['ATR_14'].iloc[-1])    
                mock_order.place_order(contract_data['symbol'], "BUY", 75, price, atm_price, description, target, stoploss)
                
            return
            
        if mock_order.is_order_long(order['id']):
            key = str(atm_price)+'-CE'
            if key not in infocus_contracts:
                return
            
            item = infocus_contracts[key]
            if item['symbol'] != order['symbol']:
                return

            target = item['ltp'] + (0.3 * item['ltp'])
            stoploss = item['ltp'] - (0.1 * item['ltp']) 

            # target = (df['close'].iloc[-1]) + (2 * df['ATR_14'].iloc[-1])
            # stoploss = (df['close'].iloc[-2]) - (1.2 * df['ATR_14'].iloc[-1])

            if order["stoploss"] < stoploss:
                mock_order.modify_order(order['id'], new_sl=stoploss, new_target=target)

        # if mock_order.is_order_short(order['id']):
        #     target = (df['close'].iloc[-1]) - (2 * df['ATR_14'].iloc[-1])
        #     stoploss = (df['close'].iloc[-2]) + (1.2 * df['ATR_14'].iloc[-1]) 

        #     if order["stoploss"] > stoploss:
        #         mock_order.modify_order(order['id'], new_sl=stoploss, new_target=target)
