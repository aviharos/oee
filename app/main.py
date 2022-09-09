# -*- coding: utf-8 -*-
# Standard Library imports
import os
import sched
import time

from Logger import getLogger
from LoopHandler import LoopHandler

logger_main = getLogger(__name__)


def get_SLEEP_TIME():
    SLEEP_TIME = os.environ.get("SLEEP_TIME")
    if SLEEP_TIME is None:
        logger_main.warning("SLEEP_TIME environment variable is not set. Using default: 60s.")
        SLEEP_TIME = 60  # seconds
        return SLEEP_TIME
    else:
        try:
            SLEEP_TIME = float(SLEEP_TIME)
            return SLEEP_TIME
        except (ValueError, TypeError):
            logger_main.critical(f"SLEEP_TIME is not a number: {os.environ.get('SLEEP_TIME')}")
            raise


SLEEP_TIME = get_SLEEP_TIME()


def loop(scheduler_):
    logger_main.info("Calculating OEE values")
    loopHandler = LoopHandler()
    loopHandler.handle()
    """
    SLEEP_TIME means the number of seconds slept
    after the end of the previous loop and the start
    of the next loop
    the OEE microservice does not follow a strict period time
    to ensure that the previous loop is always finished
    before starting a new one is due
    """
    scheduler_.enter(SLEEP_TIME, 1, loop, (scheduler_,))


def main():
    logger_main.info("Starting OEE microservice...")
    for k, v in os.environ.items():
        if "PASS" not in k:
            logger_main.info(f"environ: {k}={v}")
    scheduler = sched.scheduler(time.time, time.sleep)
    scheduler.enter(0, 1, loop, (scheduler,))
    try:
        scheduler.run()
    except KeyboardInterrupt:
        logger_main.info("KeyboardInterrupt. Stopping OEE microservice...")


if __name__ == "__main__":
    main()
