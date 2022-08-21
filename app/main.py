# -*- coding: utf-8 -*-
# Standard Library imports
import sched
import time

# Custom imports, config
from conf import conf
from Logger import getLogger
from LoopHandler import LoopHandler
logger_main = getLogger(__name__)


def loop(scheduler_):
    logger_main.info('Calculating OEE values')
    loopHandler = LoopHandler()
    try:
        loopHandler.handle()
    except Exception as error:
        # Catch any uncategorised error and log it 
        logger_main.error(error)
    finally:
        scheduler_.enter(conf['period_time'], 1, loop, (scheduler_,))


def main():
    scheduler = sched.scheduler(time.time, time.sleep)
    scheduler.enter(conf['period_time'], 1, loop, (scheduler,))
    logger_main.info('Starting OEE app...')
    try:
        scheduler.run()
    except KeyboardInterrupt:
        logger_main.info('KeyboardInterrupt. Stopping OEE app...')


if __name__ == '__main__':
    main()
