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
from pathlib import Path
import os
import sys
from common import constants
from logger import create_logger
from index.nifty50.nifty50_engine import nifty50_engine
import yaml

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

    logger.info(f"Starting orchestrator for instruments: {instruments} with strategy: {strategy}, mode: {mode}")

    if mode == constants.MOCK:
        path = Path(constants.MOCK_FOLDER_PATH)
        path.mkdir(parents=True, exist_ok=True)
    elif mode == constants.SANDBOX:
        path = Path(constants.SANDBOX_FOLDER_PATH)
        path.mkdir(parents=True, exist_ok=True)
    elif mode == constants.PRODUCTION:
        path = Path(constants.PROD_FOLDER_PATH)
        path.mkdir(parents=True, exist_ok=True)
    
    # if not os.path.exists(constants.PARAM_PATH):
    #     logger.error(f"Param file not found {constants.PARAM_PATH}")
    #     sys.exit(constants.FAIL_CODE)

    param_data=None
    # with open(constants.PARAM_PATH, 'r') as file:
    #     param_data = yaml.safe_load(file)

    # if param_data is None:
    #     logger.error(f"Param data not sourced path {constants.PARAM_PATH}")
    #     sys.exit(constants.FAIL_CODE)

    if instruments.lower() == constants.NIFTY50:
        asyncio.run(nifty50_engine(strategy, mode, param_data))

    # Further implementation would go here to manage the trading workflow
    # including fetching data, applying strategies, and placing orders.
    logger.info("Orchestrator setup complete.")