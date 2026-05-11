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
import sys
import common.constants as constants
from logger import create_logger
from oms.order_system_client import initialize_local_ledgers_for_modes
from utils.bot_utils import load_param_data

logger = create_logger("OrchestratorLogger")

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

    logger.info(f"Starting orchestrator for instruments: {instruments} with strategy: {strategy}, mode: {mode}")

    ledger_paths = initialize_local_ledgers_for_modes(list(constants.EXECUTION_MODES))
    logger.info(
        f"Initialized local OMS ledgers under {constants.TRENDOBOT_EXECUTION_RESULTS_DIR} "
        f"for modes: {', '.join(ledger_paths.keys())}"
    )

    param_data = load_param_data(mode)
    if param_data is None:
        logger.error("Param data not found in Helm-provided environment config or local param file.")
        sys.exit(constants.FAIL_CODE)

    if instruments.lower() == constants.NIFTY50:
        from index.nifty50.nifty50_engine import nifty50_engine

        asyncio.run(nifty50_engine(strategy, mode, param_data))

    # Further implementation would go here to manage the trading workflow
    # including fetching data, applying strategies, and placing orders.
    logger.info("Orchestrator setup complete.")
