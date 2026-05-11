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
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]


def _first_env(*names: str) -> str:
    for name in names:
        value = os.getenv(name, "").strip()
        if value:
            return value
    return ""


DEFAULT_TRENDOBOT_FILES_DIR = "files"
TRENDOBOT_FILES_DIR = (
    _first_env("TRENDOBOT_FILES_DIR", "SOLOBOT_FILES_DIR")
    or DEFAULT_TRENDOBOT_FILES_DIR
).rstrip("/") or DEFAULT_TRENDOBOT_FILES_DIR
if tuple(part for part in TRENDOBOT_FILES_DIR.split("/") if part) in {
    ("bots", "trendobot", "files"),
    ("bots", "solobot", "files"),
}:
    TRENDOBOT_FILES_DIR = DEFAULT_TRENDOBOT_FILES_DIR

# Keep these aliases so the shared OMS client copied from solobot can use the
# same path helpers while this package remains trendobot-named at the edge.
SOLOBOT_FILES_DIR = TRENDOBOT_FILES_DIR

TRENDOBOT_EXECUTION_RESULTS_DIR = os.path.join(TRENDOBOT_FILES_DIR, "execution_results")
SOLOBOT_EXECUTION_RESULTS_DIR = TRENDOBOT_EXECUTION_RESULTS_DIR

TREND_FILE = os.path.join(TRENDOBOT_FILES_DIR, "trend.json")
NIFTY50_OPTION_CONTRACTS_FILE = os.path.join(TRENDOBOT_FILES_DIR, "nifty50_option_contracts.json")
UPSTOX_NSE_INSTRUMENT_FILE = os.path.join(TRENDOBOT_FILES_DIR, "upstox_nse_instruments.json")
HOLIDAY_LIST_FILE = os.path.join(TRENDOBOT_FILES_DIR, "holiday_list.json")
PARAM_PATH = _first_env("TRENDOBOT_PARAM_FILE", "TRENDOBOT_PARAM_PATH") or os.path.join(TRENDOBOT_FILES_DIR, "param.yaml")

NIFTY50 = "nifty50"
NIFTY50_SYMBOL = "NSE_INDEX|Nifty 50"

SUCCESS = "success"
FAIL = "fail"
COMPLETE = "complete"
SUCCESS_CODE = 1
FAIL_CODE = 0

MARKET_START_MINUTE = 91500
MARKET_END_MINUTE = 153000

BULLISH = "bullish"
BEARISH = "bearish"
SIDEWAYS = "sideways"
SUPER_BULLISH = "super_bullish"
SUPPER_BEARISH = "super_bearish"
GAP_UP = "GAP_UP"
GAP_DOWN = "GAP_DOWN"
FLAT = "FLAT"

PROD_FOLDER_PATH = os.path.join(TRENDOBOT_EXECUTION_RESULTS_DIR, "prod")
ORDER_PROD_EVENT_LOG = os.path.join(PROD_FOLDER_PATH, "order_event_log.json")
ORDER_PROD_LOG = os.path.join(PROD_FOLDER_PATH, "order_log.csv")
ORDER_PROD_STATUS_LOG = os.path.join(PROD_FOLDER_PATH, "order_status_log.csv")
DAILY_PROD_PNL = os.path.join(PROD_FOLDER_PATH, "daily_pnl.csv")

MOCK_FOLDER_PATH = os.path.join(TRENDOBOT_EXECUTION_RESULTS_DIR, "mock")
ORDER_MOCK_EVENT_LOG = os.path.join(MOCK_FOLDER_PATH, "order_event_log.json")
ORDER_MOCK_LOG = os.path.join(MOCK_FOLDER_PATH, "order_log.csv")
ORDER_MOCK_STATUS_LOG = os.path.join(MOCK_FOLDER_PATH, "order_status_log.csv")
DAILY_MOCK_PNL = os.path.join(MOCK_FOLDER_PATH, "daily_pnl.csv")

SANDBOX_FOLDER_PATH = os.path.join(TRENDOBOT_EXECUTION_RESULTS_DIR, "sandbox")
ORDER_SANDBOX_EVENT_LOG = os.path.join(SANDBOX_FOLDER_PATH, "order_event_log.json")
ORDER_SANDBOX_LOG = os.path.join(SANDBOX_FOLDER_PATH, "order_log.csv")
ORDER_SANDBOX_STATUS_LOG = os.path.join(SANDBOX_FOLDER_PATH, "order_status_log.csv")
DAILY_SANDBOX_PNL = os.path.join(SANDBOX_FOLDER_PATH, "daily_pnl.csv")

ORDER_EVENT_LOG = os.path.join(TRENDOBOT_FILES_DIR, "order_event_log.json")
ORDER_LOG = os.path.join(TRENDOBOT_FILES_DIR, "order_log.csv")
DAILY_PNL = os.path.join(TRENDOBOT_FILES_DIR, "daily_pnl.csv")

UPSTOX_API_ACCESS_TOKEN = "upstox_api_access_token"
UPSTOX_SANDBOX_API_ACCESS_TOKEN = "upstox_sandbox_api_access_token"
UPSTOX_NSE_INSTRUMENT_FQDN = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"

# Execution mode
SANDBOX = "sandbox"
MOCK = "mock"
PRODUCTION = "production"
EXECUTION_MODES = (MOCK, SANDBOX, PRODUCTION)


def normalize_execution_mode(mode):
    normalized = str(mode or "").strip().lower()
    if normalized == "prod":
        return PRODUCTION
    return normalized


def resolve_execution_mode(fallback_mode=None):
    mode = _first_env("TRENDOBOT_MODE", "SOLOBOT_MODE") or fallback_mode or os.getenv("APP_MODE") or MOCK
    normalized = normalize_execution_mode(mode)
    if normalized not in EXECUTION_MODES:
        allowed = ", ".join(EXECUTION_MODES)
        raise ValueError(f"execution mode must be one of: {allowed}")
    return normalized

# NATS Configuration
NATS_URL = os.getenv("NATS_URL", "nats://localhost:4222")
NATS_SUBJECT_INSTRUMENT_KEYS = "marketfeeder.instrument_keys"
NATS_SUBJECT_TICK_DATA = "marketfeeder.tick_data"
NATS_SUBJECT_ADD_INSTRUMENTS = "marketfeeder.add_instruments"
NATS_SUBJECT_REMOVE_INSTRUMENTS = "marketfeeder.remove_instruments"

# Params
SMA_LONG = 20
SMA_SHORT = 5

# Strategy constants
SMA_CROSSOVER = "sma_crossover"
RSI_EMA_ANGLE = "rsi_ema_angle"
PCR_VWAP_EMA = "pcr_vwap_ema"
VWMA_EMA_ST = "vwma_ema_st"

BUY = "BUY"
SELL = "SELL"
SL = "SL"
SL_M = "SL-M"
MARKET = "MARKET"

CALL = "CALL"
CE = "CE"
PUT = "PUT"
PE = "PE"
WAITING = "WAITING"
OPEN = "OPEN"
TARGET_HIT = "TARGET HIT"
STOPLOSS_HIT = "STOPLOSS HIT"
MANUAL_EXIT = "MANUAL EXIT"
EOD_SQUARE_OFF = "EOD_SQUARE_OFF"
