
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
==================================================
 File:        constants.py
 Author:      Amit Mohanty
 
 Notes:
    - define all constants here.
==================================================
"""

import os

SOLOBOT_FILES_DIR = os.getenv("SOLOBOT_FILES_DIR", "bots/solobot/files").rstrip("/") or "bots/solobot/files"
SOLOBOT_EXECUTION_RESULTS_DIR = os.path.join(SOLOBOT_FILES_DIR, "execution_results")

TREND_FILE = os.path.join(SOLOBOT_FILES_DIR, "trend.json")
NIFTY50_OPTION_CONTRACTS_FILE = os.path.join(SOLOBOT_FILES_DIR, "nifty50_option_contracts.json")
UPSTOX_NSE_INSTRUMENT_FILE = os.path.join(SOLOBOT_FILES_DIR, "upstox_nse_instruments.json")
HOLIDAY_LIST_FILE = os.path.join(SOLOBOT_FILES_DIR, "holiday_list.json")
ORB_STATE_FILE = os.path.join(SOLOBOT_FILES_DIR, "orb_state.json")

NIFTY50="nifty50"
NIFTY50_SYMBOL = "NSE_INDEX|Nifty 50"

SUCCESS = "success"
FAIL = "fail"
COMPLETE="complete"
SUCCESS_CODE=1
FAIL_CODE =0

BULLISH = "bullish"
BEARISH = "bearish"
SIDEWAYS = "sideways"
SUPER_BULLISH = "super_bullish"
SUPPER_BEARISH = "super_bearish"
GAP_UP = "GAP_UP"
GAP_DOWN = "GAP_DOWN"
FLAT="FLAT"

PROD_FOLDER_PATH=os.path.join(SOLOBOT_EXECUTION_RESULTS_DIR, "prod")
ORDER_PROD_EVENT_LOG = os.path.join(PROD_FOLDER_PATH, "order_event_log.json")
ORDER_PROD_LOG = os.path.join(PROD_FOLDER_PATH, "order_log.csv")
ORDER_PROD_STATUS_LOG = os.path.join(PROD_FOLDER_PATH, "order_status_log.csv")
DAILY_PROD_PNL = os.path.join(PROD_FOLDER_PATH, "daily_pnl.csv")

MOCK_FOLDER_PATH=os.path.join(SOLOBOT_EXECUTION_RESULTS_DIR, "mock")
ORDER_MOCK_EVENT_LOG = os.path.join(MOCK_FOLDER_PATH, "order_event_log.json")
ORDER_MOCK_LOG = os.path.join(MOCK_FOLDER_PATH, "order_log.csv")
ORDER_MOCK_STATUS_LOG = os.path.join(MOCK_FOLDER_PATH, "order_status_log.csv")
DAILY_MOCK_PNL = os.path.join(MOCK_FOLDER_PATH, "daily_pnl.csv")

SANDBOX_FOLDER_PATH=os.path.join(SOLOBOT_EXECUTION_RESULTS_DIR, "sandbox")

# NATS Configuration
NATS_URL = os.getenv("NATS_URL", "nats://localhost:4222")
NATS_SUBJECT_INSTRUMENT_KEYS = "marketfeeder.instrument_keys"
NATS_SUBJECT_TICK_DATA = "marketfeeder.tick_data"
NATS_SUBJECT_ADD_INSTRUMENTS = "marketfeeder.add_instruments"
NATS_SUBJECT_REMOVE_INSTRUMENTS = "marketfeeder.remove_instruments"
ORDER_SANDBOX_EVENT_LOG = os.path.join(SANDBOX_FOLDER_PATH, "order_event_log.json")
ORDER_SANDBOX_LOG = os.path.join(SANDBOX_FOLDER_PATH, "order_log.csv")
ORDER_SANDBOX_STATUS_LOG = os.path.join(SANDBOX_FOLDER_PATH, "order_status_log.csv")
DAILY_SANDBOX_PNL = os.path.join(SANDBOX_FOLDER_PATH, "daily_pnl.csv")

UPSTOX_API_ACCESS_TOKEN = "upstox_api_access_token"
UPSTOX_SANDBOX_API_ACCESS_TOKEN = "upstox_sandbox_api_access_token"
UPSTOX_NSE_INSTRUMENT_FQDN = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"

PARAM_PATH = os.path.join(SOLOBOT_FILES_DIR, "param.yaml")

#Execution mode
SANDBOX = "sandbox"
MOCK="mock"
PRODUCTION="production"

#Params
SMA_LONG = 20
SMA_SHORT = 5

# Strategy constants
SMA_CROSSOVER = "sma_crossover" 
RSI_EMA_ANGLE = "rsi_ema_angle"
ORB = "opening_range_breakout"
RSI_EMA_ANGLE = "rsi_ema_angle"
PCR_VWAP_EMA_ORB="pcr_vwap_ema_orb"

BUY="BUY"
SELL="SELL"
SL="SL"
SL_M="SL-M"
MARKET="MARKET"

CALL="CALL"
CE="CE"
PUT="PUT"
PE="PE"
WAITING="WAITING"
OPEN="OPEN"
TARGET_HIT="TARGET HIT"
STOPLOSS_HIT="STOPLOSS HIT"
MANUAL_EXIT="MANUAL EXIT"
EOD_SQUARE_OFF="EOD_SQUARE_OFF"

BULLISH="bullish"
SIDEWAYS="sideways"
BEARISH="bearish"

