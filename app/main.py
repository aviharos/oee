# -*- coding: utf-8 -*-
# Standard Library imports
import os
import sched
import time

from Logger import getLogger
from LoopHandler import LoopHandler
logger_main = getLogger(__name__)

# Load environment variables
PERIOD_TIME = os.environ.get('PERIOD_TIME')
if PERIOD_TIME is None:
    PERIOD_TIME = 60 # seconds


def loop(scheduler_):
    logger_main.info('Calculating OEE values')
    loopHandler = LoopHandler()
    try:
        loopHandler.handle()
    except Exception as error:
        # Catch any uncategorised error and log it 
        logger_main.error(error)
    finally:
        scheduler_.enter(PERIOD_TIME, 1, loop, (scheduler_,))


def main():
    scheduler = sched.scheduler(time.time, time.sleep)
    scheduler.enter(PERIOD_TIME, 1, loop, (scheduler,))
    logger_main.info('Starting OEE app...')
    try:
        scheduler.run()
    except KeyboardInterrupt:
        logger_main.info('KeyboardInterrupt. Stopping OEE app...')


if __name__ == '__main__':
    main()
