#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
==================================================
 File:        main.py
 Author:      Amit Mohanty
 
 Notes:
    - Trigger the orchestrator with command line arguments.
    - Takes instruments as a parameter.
==================================================
"""

import sys
import pathlib
import argparse

# Add current directory to sys.path so relative imports work
TRENDOBOT_DIR = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(TRENDOBOT_DIR))

from logger import create_logger
from index.orchestrator import orchestrator
from common import constants

logger = create_logger("TrendBotMain")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Take parameters from the command line.")
    parser.add_argument("-i", "--instruments", help="Instrument Name")
    parser.add_argument("-s", "--strategy", help="Strategy Name")
    parser.add_argument(
        "-l",
        "--level",
        help="Execution Mode fallback when TRENDOBOT_MODE/SOLOBOT_MODE is not set",
        choices=constants.EXECUTION_MODES,
    )

    args = parser.parse_args()
    instruments = args.instruments
    strategy = args.strategy
    try:
        mode = constants.resolve_execution_mode(args.level)
    except ValueError as exc:
        parser.error(str(exc))

    logger.info(f"Received instruments: {instruments}")
    logger.info(f"Mode: {mode}")

    orchestrator(instruments, strategy, mode=mode)
