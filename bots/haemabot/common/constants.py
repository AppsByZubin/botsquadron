#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Common constants for haemabot.
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


DEFAULT_HAEMABOT_FILES_DIR = "files"
HAEMABOT_FILES_DIR = (
    _first_env("HAEMABOT_FILES_DIR", "HEMABOT_FILES_DIR", "BOT_FILES_DIR")
    or DEFAULT_HAEMABOT_FILES_DIR
).rstrip("/") or DEFAULT_HAEMABOT_FILES_DIR

if tuple(part for part in HAEMABOT_FILES_DIR.split("/") if part) in {
    ("bots", "haemabot", "files"),
    ("bots", "hemabot", "files"),
}:
    HAEMABOT_FILES_DIR = DEFAULT_HAEMABOT_FILES_DIR

HAEMABOT_EXECUTION_RESULTS_DIR = os.path.join(HAEMABOT_FILES_DIR, "execution_results")

# Aliases used by the shared OMS client shape copied from other bots.
HEMABOT_FILES_DIR = HAEMABOT_FILES_DIR
HEMABOT_EXECUTION_RESULTS_DIR = HAEMABOT_EXECUTION_RESULTS_DIR
TRENDOBOT_EXECUTION_RESULTS_DIR = HAEMABOT_EXECUTION_RESULTS_DIR
SOLOBOT_EXECUTION_RESULTS_DIR = HAEMABOT_EXECUTION_RESULTS_DIR

PARAM_PATH = (
    _first_env(
        "HAEMABOT_PARAM_FILE",
        "HAEMABOT_PARAM_PATH",
        "HEMABOT_PARAM_FILE",
        "HEMABOT_PARAM_PATH",
        "PARAM_FILE",
        "PARAM_PATH",
    )
    or os.path.join(HAEMABOT_FILES_DIR, "param.yaml")
)

TREND_FILE = os.path.join(HAEMABOT_FILES_DIR, "trend.json")
NIFTY50_OPTION_CONTRACTS_FILE = os.path.join(HAEMABOT_FILES_DIR, "nifty50_option_contracts.json")
UPSTOX_NSE_INSTRUMENT_FILE = os.path.join(HAEMABOT_FILES_DIR, "upstox_nse_instruments.json")
HOLIDAY_LIST_FILE = os.path.join(HAEMABOT_FILES_DIR, "holiday_list.json")

NIFTY50 = "nifty50"
NIFTY50_SYMBOL = "NSE_INDEX|Nifty 50"
SUPPORTED_INSTRUMENTS = (NIFTY50,)

HM_EMA_ADX = "hm_ema_adx"
DEFAULT_STRATEGY = HM_EMA_ADX

SUCCESS = "success"
FAIL = "fail"
COMPLETE = "complete"
SUCCESS_CODE = 1
FAIL_CODE = 0

MARKET_START_MINUTE = 91500
MARKET_END_MINUTE = 153000

MOCK_FOLDER_PATH = os.path.join(HAEMABOT_EXECUTION_RESULTS_DIR, "mock")
ORDER_MOCK_EVENT_LOG = os.path.join(MOCK_FOLDER_PATH, "order_event_log.json")
ORDER_MOCK_LOG = os.path.join(MOCK_FOLDER_PATH, "order_log.csv")
ORDER_MOCK_STATUS_LOG = os.path.join(MOCK_FOLDER_PATH, "order_status_log.csv")
DAILY_MOCK_PNL = os.path.join(MOCK_FOLDER_PATH, "daily_pnl.csv")

SANDBOX_FOLDER_PATH = os.path.join(HAEMABOT_EXECUTION_RESULTS_DIR, "sandbox")
ORDER_SANDBOX_EVENT_LOG = os.path.join(SANDBOX_FOLDER_PATH, "order_event_log.json")
ORDER_SANDBOX_LOG = os.path.join(SANDBOX_FOLDER_PATH, "order_log.csv")
ORDER_SANDBOX_STATUS_LOG = os.path.join(SANDBOX_FOLDER_PATH, "order_status_log.csv")
DAILY_SANDBOX_PNL = os.path.join(SANDBOX_FOLDER_PATH, "daily_pnl.csv")

PROD_FOLDER_PATH = os.path.join(HAEMABOT_EXECUTION_RESULTS_DIR, "prod")
ORDER_PROD_EVENT_LOG = os.path.join(PROD_FOLDER_PATH, "order_event_log.json")
ORDER_PROD_LOG = os.path.join(PROD_FOLDER_PATH, "order_log.csv")
ORDER_PROD_STATUS_LOG = os.path.join(PROD_FOLDER_PATH, "order_status_log.csv")
DAILY_PROD_PNL = os.path.join(PROD_FOLDER_PATH, "daily_pnl.csv")

ORDER_EVENT_LOG = os.path.join(HAEMABOT_FILES_DIR, "order_event_log.json")
ORDER_LOG = os.path.join(HAEMABOT_FILES_DIR, "order_log.csv")
DAILY_PNL = os.path.join(HAEMABOT_FILES_DIR, "daily_pnl.csv")

UPSTOX_API_ACCESS_TOKEN = "upstox_api_access_token"
UPSTOX_SANDBOX_API_ACCESS_TOKEN = "upstox_sandbox_api_access_token"
UPSTOX_NSE_INSTRUMENT_FQDN = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"

DO_S3_ENDPOINT_URL = "DO_S3_ENDPOINT_URL"
DO_S3_REGION = "DO_S3_REGION"
DO_S3_ACCESS_KEY_ID = "DO_S3_ACCESS_KEY_ID"
DO_S3_SECRET_ACCESS_KEY = "DO_S3_SECRET_ACCESS_KEY"
DO_S3_BUCKET_NAME = "DO_S3_BUCKET_NAME"
DO_S3_SPACES_PREFIX = "DO_S3_SPACES_PREFIX"
DO_S3_REQUIRED_BUCKET_NAME = "index-bucket"
DO_S3_DEFAULT_PREFIX = "index-bucket-holder/trades"

MOCK = "mock"
SANDBOX = "sandbox"
PRODUCTION = "production"
EXECUTION_MODES = (MOCK, SANDBOX, PRODUCTION)


def normalize_execution_mode(mode):
    normalized = str(mode or "").strip().lower()
    if normalized == "prod":
        return PRODUCTION
    return normalized


def resolve_execution_mode(fallback_mode=None):
    mode = (
        _first_env("HAEMABOT_MODE", "HEMABOT_MODE", "BOT_MODE")
        or fallback_mode
        or os.getenv("APP_MODE")
        or MOCK
    )
    normalized = normalize_execution_mode(mode)
    if normalized not in EXECUTION_MODES:
        allowed = ", ".join(EXECUTION_MODES)
        raise ValueError(f"execution mode must be one of: {allowed}")
    return normalized


NATS_URL = os.getenv("NATS_URL", "nats://localhost:4222")
NATS_SUBJECT_INSTRUMENT_KEYS = "marketfeeder.instrument_keys"
NATS_SUBJECT_TICK_DATA = "marketfeeder.tick_data"
NATS_SUBJECT_ADD_INSTRUMENTS = "marketfeeder.add_instruments"
NATS_SUBJECT_REMOVE_INSTRUMENTS = "marketfeeder.remove_instruments"

BULLISH = "bullish"
BEARISH = "bearish"
SIDEWAYS = "sideways"
SUPER_BULLISH = "super_bullish"
SUPPER_BEARISH = "super_bearish"
GAP_UP = "GAP_UP"
GAP_DOWN = "GAP_DOWN"
FLAT = "FLAT"

SMA_LONG = 20
SMA_SHORT = 5
SMA_CROSSOVER = "sma_crossover"
RSI_EMA_ANGLE = "rsi_ema_angle"
PCR_VWAP_EMA = "pcr_vwap_ema"

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
KILL_SWITCH = "KILL_SWITCH"
