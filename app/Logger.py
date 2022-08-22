# -*- coding: utf-8 -*-
# Standard Library imports
import logging
import os
import sys

# get environment variables
# if they are missing, set default values
LOGGING_LEVEL = os.environ.get('LOGGING_LEVEL')
if LOGGING_LEVEL is None:
    LOGGING_LEVEL = 'DEBUG'

LOG_TO_FILE = os.environ.get('LOG_TO_FILE')
if LOG_TO_FILE is None:
    LOG_TO_FILE = False

LOG_TO_STDOUT = os.environ.get('LOG_TO_STDOUT')
if LOG_TO_STDOUT is None:
    LOG_TO_STDOUT = True

def getLogger(name):
    logging_levels = {'DEBUG': logging.DEBUG,
                      'INFO': logging.INFO,
                      'WARNING': logging.WARNING,
                      'ERROR': logging.ERROR,
                      'CRITICAL': logging.CRITICAL}
    logger = logging.getLogger(name)
    formatter = logging.Formatter('%(asctime)s:%(name)s:%(message)s')
    logger.setLevel(logging_levels[LOGGING_LEVEL])
    if LOG_TO_FILE:
        file_handler = logging.FileHandler(f'{__name__}.log')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    if LOG_TO_STDOUT:
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)
    return logger

