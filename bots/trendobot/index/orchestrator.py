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

import asyncio
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import common.constants as constants
from logger import create_logger
from oms.order_system_client import initialize_local_ledgers_for_modes
from utils.bot_utils import load_param_data, should_skip_strategy_execution

logger = create_logger("OrchestratorLogger")
IST = ZoneInfo("Asia/Kolkata")
BOT_DIR = Path(__file__).resolve().parents[1]


def _holiday_list_candidates():
    configured_path = Path(constants.HOLIDAY_LIST_FILE)
    if configured_path.is_absolute():
        return [configured_path]
    return [BOT_DIR / configured_path, Path.cwd() / configured_path]


def _holiday_list_path():
    candidates = _holiday_list_candidates()
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[0]


def _holiday_description(check_date):
    holiday_path = _holiday_list_path()
    try:
        with holiday_path.open("r", encoding="utf-8") as file:
            holidays = json.load(file)
    except FileNotFoundError:
        logger.warning(f"Holiday list file not found: {holiday_path}")
        return None
    except Exception as exc:
        logger.warning(f"Failed to read holiday list file {holiday_path}: {exc}")
        return None

    check_date_str = check_date.isoformat()
    for holiday in holidays:
        if isinstance(holiday, dict) and holiday.get("date") == check_date_str:
            return holiday.get("description") or "market holiday"
    return None


def _should_skip_for_market_calendar():
    today = datetime.now(IST).date()
    if today.weekday() >= 5:
        return True, f"{today.isoformat()} is a weekend"

    holiday_description = _holiday_description(today)
    if holiday_description:
        return True, f"{today.isoformat()} is in holiday_list.json ({holiday_description})"
    return False, ""


def orchestrator(instruments, strategy, mode=None):
    """
    Args:
        instruments (str): Instrument Name
        strategy (str): Strategy Name
        mock (bool): Flag to enable mock trading
        sandbox (bool): Flag to enable sandbox mode
            
    Notes:
    - Orchestrator to manage the trading workflow.
    """

    try:
        mode = constants.resolve_execution_mode(mode)
    except ValueError as exc:
        logger.error(str(exc))
        sys.exit(constants.FAIL_CODE)

    skip_calendar, calendar_reason = _should_skip_for_market_calendar()
    if skip_calendar:
        logger.info(f"Skipping all strategies because {calendar_reason}.")
        return False

    logger.info(f"Starting orchestrator for instruments: {instruments} with strategy: {strategy}, mode: {mode}")

    ledger_paths = initialize_local_ledgers_for_modes(list(constants.TRADING_EXECUTION_MODES))
    logger.info(
        f"Initialized local OMS ledgers under {constants.TRENDOBOT_EXECUTION_RESULTS_DIR} "
        f"for modes: {', '.join(ledger_paths.keys())}"
    )

    param_data = load_param_data(mode)
    if param_data is None:
        logger.error("Param data not found in Helm-provided environment config or local param file.")
        sys.exit(constants.FAIL_CODE)

    skip_execution, skip_reason = should_skip_strategy_execution(
        param_data,
        strategy,
        current_date=os.getenv("TRENDOBOT_CURR_DATE"),
    )
    if skip_execution:
        logger.info(f"Skipping strategy {strategy} for today because {skip_reason}.")
        return False

    if instruments.lower() == constants.NIFTY50:
        from index.nifty50.nifty50_engine import nifty50_engine

        asyncio.run(nifty50_engine(strategy, mode, param_data))
        logger.info("Orchestrator setup complete.")
        return True

    # Further implementation would go here to manage the trading workflow
    # including fetching data, applying strategies, and placing orders.
    logger.info("Orchestrator setup complete.")
    return True
