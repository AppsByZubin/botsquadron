#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Logger creation utility for haemabot.
"""

import logging
import os
import sys
from datetime import datetime


class LogFormatter(logging.Formatter):
    COLOR_CODES = {
        logging.CRITICAL: "\033[1;35m",
        logging.ERROR: "\033[1;31m",
        logging.WARNING: "\033[1;33m",
        logging.INFO: "\033[0;32m",
        logging.DEBUG: "\033[1;30m",
    }
    RESET_CODE = "\033[0m"

    def __init__(self, color, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.color = color

    def format(self, record, *args, **kwargs):
        if self.color and record.levelno in self.COLOR_CODES:
            record.color_on = self.COLOR_CODES[record.levelno]
            record.color_off = self.RESET_CODE
        else:
            record.color_on = ""
            record.color_off = ""
        return super().format(record, *args, **kwargs)


def create_logger(name):
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)
    format_str = "%(color_on)s%(asctime)s - %(levelname)s - %(name)s - %(lineno)d - %(message)s%(color_off)s"

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(LogFormatter(fmt=format_str, color=True))
    logger.addHandler(console_handler)

    log_dir = os.getenv("HAEMABOT_LOG_DIR") or os.getenv("HEMABOT_LOG_DIR") or os.getenv("BOT_LOG_DIR") or "logs"
    os.makedirs(log_dir, exist_ok=True)
    log_filename = os.path.join(log_dir, f"{datetime.now().strftime('%d-%m-%y')}_haemabot.log")

    file_handler = logging.FileHandler(log_filename)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(LogFormatter(fmt=format_str, color=False))
    logger.addHandler(file_handler)

    return logger
