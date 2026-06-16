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
SOLOBOT_DIR = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(SOLOBOT_DIR))

from logger import create_logger
from common import constants

logger = create_logger("SoloBotMain")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Take parameters from the command line.")
    parser.add_argument("-i", "--instruments", help="Instrument Name")
    parser.add_argument("-s", "--strategy", help="Strategy Name")
    parser.add_argument(
        "-l",
        "--level",
        help="Execution Mode fallback when SOLOBOT_MODE is not set",
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

    if mode == constants.REST:
        logger.info("Rest mode enabled; exiting without starting orchestrator.")
        sys.exit(0)

    from index.orchestrator import orchestrator
    from utils.s3_upload_utils import upload_trade_artifacts_to_s3

    did_run = orchestrator(instruments, strategy, mode=mode)
    if did_run is False:
        logger.info("Skipping S3 upload because strategy execution was skipped.")
    else:
        upload_trade_artifacts_to_s3(bot_name="solobot", execution_mode=mode)
