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
from index.orchestrator import orchestrator
from common import constants

logger = create_logger("SoloBotMain")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Take parameters from the command line.")
    parser.add_argument("-i", "--instruments", help="Instrument Name")
    parser.add_argument("-s", "--strategy", help="Strategy Name")
    parser.add_argument("-l","--level", help="Execution Mode", choices=[constants.MOCK, constants.SANDBOX, constants.PRODUCTION])

    args = parser.parse_args()
    instruments = args.instruments
    strategy = args.strategy

    logger.info(f"Received instruments: {instruments}")
    logger.info(f"Mode: {args.level}")

    orchestrator(instruments, strategy, mode=args.level)
