# -*- coding: utf-8 -*-
"""
Created on Thu Jun 16 12:58:03 2022

@author: Andor
"""
# Standard Library imports
import logging
import sys

# custom imports
from conf import conf


def getLogger(name):
    logging_levels = {'DEBUG': logging.DEBUG,
                      'INFO': logging.INFO,
                      'WARNING': logging.WARNING,
                      'ERROR': logging.ERROR,
                      'CRITICAL': logging.CRITICAL}
    logger = logging.getLogger(name)
    formatter = logging.Formatter('%(asctime)s:%(name)s:%(message)s')
    logger.setLevel(logging_levels[conf['logging_level']])
    if conf['log_to_file']:
        file_handler = logging.FileHandler(f'{__name__}.log')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    if conf['log_to_stdout']:
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)
    return logger
