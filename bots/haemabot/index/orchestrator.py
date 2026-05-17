#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Orchestrates haemabot startup.
"""

import asyncio
import sys

from common import constants
from logger import create_logger
from oms.order_system_client import initialize_local_ledgers_for_modes
from utils.bot_utils import load_param_data

logger = create_logger("OrchestratorLogger")


def orchestrator(instruments, strategy, mode=None):
    try:
        mode = constants.resolve_execution_mode(mode)
    except ValueError as exc:
        logger.error(str(exc))
        sys.exit(constants.FAIL_CODE)

    instruments = (instruments or constants.NIFTY50).strip().lower()
    strategy = (strategy or constants.DEFAULT_STRATEGY).strip().lower()

    logger.info(
        f"Starting haemabot orchestrator for instruments: {instruments}, strategy: {strategy}, mode: {mode}"
    )

    ledger_paths = initialize_local_ledgers_for_modes(list(constants.EXECUTION_MODES))
    logger.info(
        f"Initialized local OMS ledgers under {constants.HAEMABOT_EXECUTION_RESULTS_DIR} "
        f"for modes: {', '.join(ledger_paths.keys())}"
    )

    param_data = load_param_data(mode) or {}
    if not param_data:
        logger.info("No haemabot params configured yet; continuing with defaults.")

    if instruments == constants.NIFTY50:
        from index.nifty50.nifty50_engine import nifty50_engine

        asyncio.run(nifty50_engine(strategy, mode, param_data))
        logger.info("Haemabot orchestrator setup complete.")
        return

    supported = ", ".join(constants.SUPPORTED_INSTRUMENTS)
    logger.error(f"Unsupported instrument group: {instruments}. Supported: {supported}")
    sys.exit(constants.FAIL_CODE)
