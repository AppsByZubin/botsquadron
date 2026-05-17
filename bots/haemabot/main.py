#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Command-line entrypoint for haemabot.
"""

import argparse
import pathlib
import sys

HAEMABOT_DIR = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HAEMABOT_DIR))

from common import constants
from index.orchestrator import orchestrator
from logger import create_logger
from utils.s3_upload_utils import maybe_upload_trade_artifacts_to_s3

logger = create_logger("HaemaBotMain")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run haemabot.")
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
        help="Execution mode fallback when HAEMABOT_MODE is not set.",
    )

    args = parser.parse_args()
    try:
        mode = constants.resolve_execution_mode(args.level)
    except ValueError as exc:
        parser.error(str(exc))

    logger.info(f"Received instruments: {args.instruments}")
    logger.info(f"Received strategy: {args.strategy}")
    logger.info(f"Mode: {mode}")

    orchestrator(args.instruments, args.strategy, mode=mode)
    maybe_upload_trade_artifacts_to_s3(bot_name="haemabot", execution_mode=mode)
