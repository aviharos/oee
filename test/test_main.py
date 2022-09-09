# -*- coding: utf-8 -*-
# Standard Library imports
from datetime import datetime
import copy
import os
import sched
import sys
import time
import unittest
from unittest.mock import patch

# Custom imports
sys.path.insert(0, os.path.join("..", "app"))
import main
from LoopHandler import LoopHandler


class test_object_to_template(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.scheduler = sched.scheduler(time.time, time.sleep)

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_SLEEP_TIME(self):
        self.assertEqual(60, main.SLEEP_TIME)

    def test_loop_exceptions(self):
        # see if the loop can proceed even if exceptions are raised
        for exception in (
            AssertionError,
            AttributeError,
            MemoryError,
            NameError,
            NotImplementedError,
            OSError,
            SyntaxError,
        ):
            with patch("LoopHandler.LoopHandler.handle()") as mock_handle:
                mock_handle.side_effect = exception
                try:
                    main.loop()
                except Exception as error:
                    self.fail(f"main.loop() raised {error} unexpectedly")

        # KeyboardInterrupt is not stopped
        with patch("LoopHandler.LoopHandler.handle()") as mock_handle:
            mock_handle.side_effect = KeyboardInterrupt
            with self.assertRaises(KeyboardInterrupt):
                main.loop()

    @patch(f"{LoopHandler.LoopHandler.__name__}.datetime", wraps=datetime)
    def test_loop_and_schedule_event(self, mock_datetime):
        mock_datetime
        pass
        # logger_main.info("Calculating OEE values")
        # loopHandler = LoopHandler()
        # try:
        #     loopHandler.handle()
        # except Exception as error:
        #     # Catch any uncategorised error and log it
        #     logger_main.error(error)
        # finally:
        #     # SLEEP_TIME means the number of seconds slept between 2 loops
        #     # the OEE microservice does not follow a strict period time
        #     # to ensure that the previous loop is always finished
        #     # before starting a new one is due
        #     scheduler_.enter(SLEEP_TIME, 1, loop, (scheduler_,))
        #     pass

    def test_main(self):
        pass
        # logger_main.info("Starting OEE microservice...")
        # for k, v in os.environ.items():
        #     if "PASS" not in k:
        #         logger_main.info(f"environ: {k}={v}")
        # scheduler = sched.scheduler(time.time, time.sleep)
        # scheduler.enter(0, 1, loop, (scheduler,))
        # try:
        #     scheduler.run()
        # except KeyboardInterrupt:
        #     logger_main.info("KeyboardInterrupt. Stopping OEE microservice...")
        #     pass


def test_main():
    unittest.main()


if __name__ == "__main__":
    test_main()
