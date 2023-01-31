# -*- coding: utf-8 -*-
"""The main file for the OEE and Throughput calculator microservice

It reads the SLEEP_TIME environment variable.
The main module runs the LoopHandler each loop, using a period of about SLEEP_TIME.
See the loop function's docs for why this time is not exact.

Each loop, the LoopHandler calculates and updates the OEE and Throughput objects.
"""
# Standard Library imports
import os
import sched
import time

from Logger import getLogger
from LoopHandler import LoopHandler

logger_main = getLogger(__name__)


def get_SLEEP_TIME() -> int:
    """Read and convert the SLEEP_TIME environment variable

    If the SLEEP_TIME is None, the default of 60 seconds is used.

    Returns:
        the SLEEP_TIME as a float

    Raises:
        ValueError or TypeError: if the SLEEP_TIME cannot be converted to float
    """
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


def loop(scheduler_: sched.scheduler):
    """The main loop, that runs each cycle

    SLEEP_TIME means the number of seconds slept
    after the end of the previous loop and the start
    of the next loop.
    The OEE microservice does not follow a strict period time
    to ensure that the previous loop is always finished
    before starting a new one is due.

    Args:
        scheduler_ (sched.scheduler): instance of sched.scheduler, used in all loops
    """
    logger_main.info("Calculating OEE and Throughput values")
    loopHandler = LoopHandler()
    loopHandler.handle()
    scheduler_.enter(SLEEP_TIME, 1, loop, (scheduler_,))


def main():
    """The main module that starts up the microservice and runs the first loop

    All environment variables not containing "PASS" or "KEY" are logged for information
    Can be stopped with KeyboardInterrupt
    """
    logger_main.info("Starting OEE microservice...")
    for k, v in os.environ.items():
        if "PASS" not in k and "KEY" not in k:
            logger_main.debug(f"environ: {k}={v}")
    scheduler = sched.scheduler(time.time, time.sleep)
    scheduler.enter(0, 1, loop, (scheduler,))
    try:
        scheduler.run()
    except KeyboardInterrupt:
        logger_main.info("KeyboardInterrupt. Stopping OEE microservice...")


if __name__ == "__main__":
    main()
