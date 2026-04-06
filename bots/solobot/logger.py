#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
==================================================
 File:        logger.py
 Author:      Amit Mohanty
 
 Notes:
    - logger creation utility with colorized console output and file logging.
==================================================
"""

import logging
import sys
import os
from datetime import datetime

# Logging formatter supporting colorized output
class LogFormatter(logging.Formatter):

    COLOR_CODES = {
        logging.CRITICAL: "\033[1;35m", # bright/bold magenta
        logging.ERROR:    "\033[1;31m", # bright/bold red
        logging.WARNING:  "\033[1;33m", # bright/bold yellow
        logging.INFO:     "\033[0;32m", # green
        logging.DEBUG:    "\033[1;30m"  # bright/bold dark gray
    }

    RESET_CODE = "\033[0m"

    def __init__(self, color, *args, **kwargs):
        super(LogFormatter, self).__init__(*args, **kwargs)
        self.color = color

    def format(self, record, *args, **kwargs):
        if (self.color == True and record.levelno in self.COLOR_CODES):
            record.color_on  = self.COLOR_CODES[record.levelno]
            record.color_off = self.RESET_CODE
        else:
            record.color_on  = ""
            record.color_off = ""
        return super(LogFormatter, self).format(record, *args, **kwargs)

def create_logger(name):
    """
    Create and return a logger using logging module with given name.
    """ 

    logger = logging.getLogger(name)

    logger.setLevel(logging.DEBUG)
    format_str = '%(color_on)s%(asctime)s - %(levelname)s - %(name)s - %(lineno)d - %(message)s%(color_off)s'

    # Console handler with color
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG)
    console_formatter = LogFormatter(fmt=format_str, color=True)
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    # File handler without color
    os.makedirs("logs", exist_ok=True)
    log_filename = f"logs/{datetime.now().strftime('%d-%m-%y_solobot.log')}"
    file_handler = logging.FileHandler(log_filename)
    file_handler.setLevel(logging.DEBUG)
    file_formatter = LogFormatter(fmt=format_str, color=False)
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    # Success
    return logger
