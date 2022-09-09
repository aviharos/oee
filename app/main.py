# -*- coding: utf-8 -*-
# Standard Library imports
import os
import sched
import time

from Logger import getLogger
from LoopHandler import LoopHandler

logger_main = getLogger(__name__)

# Load environment variables
SLEEP_TIME = os.environ.get("SLEEP_TIME")
if SLEEP_TIME is None:
    logger_main.warning("SLEEP_TIME environment variable is not set. Using default: 60s.")
    SLEEP_TIME = 60  # seconds
else:
    try:
        SLEEP_TIME = float(SLEEP_TIME)
    except Exception as error:
        logger_main.warning(f"SLEEP_TIME environment variable is not a number. Using default: 60s.{error}")
        SLEEP_TIME = 60


def loop(scheduler_):
    logger_main.info("Calculating OEE values")
    try:
        loopHandler = LoopHandler()
        loopHandler.handle()
    except (KeyboardInterrupt,
            SystemExit):
        raise KeyboardInterrupt
    except Exception as error:
        # Catch any uncategorised error and log it
        logger_main.error(error)
    finally:
        # SLEEP_TIME means the number of seconds slept between 2 loops
        # the OEE microservice does not follow a strict period time
        # to ensure that the previous loop is always finished
        # before starting a new one is due
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
