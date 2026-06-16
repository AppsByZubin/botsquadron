#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Command-line entrypoint for fibobot.
"""

import argparse
import pathlib
import sys

FIBOBOT_DIR = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(FIBOBOT_DIR))

from common import constants
from logger import create_logger

logger = create_logger("FibobotMain")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run fibobot.")
    parser.add_argument(
        "-i",
        "--instruments",
        default=constants.NIFTY50,
        help="Instrument group to run, for example nifty50.",
    )
    parser.add_argument(
        "-s",
        "--strategy",
        default=constants.DEFAULT_STRATEGY,
        help="Strategy name.",
    )
    parser.add_argument(
        "-l",
        "--level",
        choices=constants.EXECUTION_MODES,
        help="Execution mode fallback when FIBOBOT_MODE is not set.",
    )

    args = parser.parse_args()
    try:
        mode = constants.resolve_execution_mode(args.level)
    except ValueError as exc:
        parser.error(str(exc))

    logger.info(f"Received instruments: {args.instruments}")
    logger.info(f"Received strategy: {args.strategy}")
    logger.info(f"Mode: {mode}")

    if mode == constants.REST:
        logger.info("Rest mode enabled; exiting without starting orchestrator.")
        sys.exit(0)

    from index.orchestrator import orchestrator
    from utils.s3_upload_utils import upload_trade_artifacts_to_s3

    did_run = orchestrator(args.instruments, args.strategy, mode=mode)
    if did_run is False:
        logger.info("Skipping S3 upload because strategy execution was skipped.")
    else:
        upload_trade_artifacts_to_s3(bot_name="fibobot", execution_mode=mode)
