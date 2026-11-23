#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Logger creation utility for fibobot.
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


_stale_logs_checked = False


def _remove_stale_logs(log_dir, current_log_name):
    """Delete this bot's dated logs (dd-mm-yy_<bot>.log) except the current one, once per process."""
    global _stale_logs_checked
    if _stale_logs_checked:
        return []
    _stale_logs_checked = True
    from fnmatch import fnmatch

    suffix = current_log_name.split("_", 1)[1]
    removed = []
    for name in sorted(os.listdir(log_dir)):
        path = os.path.join(log_dir, name)
        if name == current_log_name or not fnmatch(name, f"[0-9][0-9]-[0-9][0-9]-[0-9][0-9]_{suffix}"):
            continue
        if os.path.isfile(path) and not os.path.islink(path):
            try:
                os.remove(path)
                removed.append(name)
            except OSError:
                pass
    return removed


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

    log_dir = os.getenv("FIBOBOT_LOG_DIR") or os.getenv("BOT_LOG_DIR") or "logs"
    os.makedirs(log_dir, exist_ok=True)
    log_filename = os.path.join(log_dir, f"{datetime.now().strftime('%d-%m-%y')}_fibobot.log")
    removed_logs = _remove_stale_logs(log_dir, os.path.basename(log_filename))

    file_handler = logging.FileHandler(log_filename)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(LogFormatter(fmt=format_str, color=False))
    logger.addHandler(file_handler)
    if removed_logs:
        logger.info("Removed %s stale log file(s): %s", len(removed_logs), ", ".join(removed_logs))

    return logger
