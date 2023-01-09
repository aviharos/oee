# -*- coding: utf-8 -*-
# Standard Library imports
from datetime import datetime
import copy
import json
from logging import getLoggerClass
import os
import sched
import sys
import time
import unittest
from unittest.mock import patch, Mock

# PyPI imports
# import numpy as np
import pandas as pd
# import psycopg2
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.types import Text

# Custom imports
sys.path.insert(0, os.path.join("..", "src"))
from modules.remove_orion_metadata import remove_orion_metadata
from modules import reupload_jsons_to_Orion
from modules.assertDeepAlmostEqual import assertDeepAlmostEqual
from Logger import getLogger
import main
import OEE
import Orion

POSTGRES_HOST = os.environ.get("POSTGRES_HOST")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT")
POSTGRES_USER = os.environ.get("POSTGRES_USER")
POSTGRES_SCHEMA = os.environ.get("POSTGRES_SCHEMA")

# Constants
PLACES = 5
WORKSTATION_ID = "urn:ngsiv2:i40Asset:Workstation1"
WORKSTATION_TABLE = WORKSTATION_ID.lower().replace(":", "_") + "_i40asset"
WORKSTATION_FILE = f"{WORKSTATION_TABLE}.csv"
JOB_ID = "urn:ngsiv2:i40Process:Job202200045"
JOB_TABLE = JOB_ID.lower().replace(":", "_") + "_i40process"
JOB_FILE = f"{JOB_TABLE}.csv"


class test_object_to_template(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """
        Almost all of this setUpClass is identical to the test_OEECalculator object's setUpClass
        """
        cls.scheduler = sched.scheduler(time.time, time.sleep)
        cls.logger = getLogger(__name__)
        cls.blank_oee = {
            "id": "urn:ngsiv2:i40Asset:OEE1",
            "type": "i40Asset",
            "i40AssetType": {"type": "Text", "value": "OEE"},
            "RefWorkstation": {"type": "Relationship", "value": "urn:ngsiv2:i40Asset:Workstation1"},
            "refJob": {"type": "Relationship", "value": "urn:ngsiv2:i40Process:Job202200045"},
            "availability": {"type": "Number", "value": None},
            "performance": {"type": "Number", "value": None},
            "quality": {"type": "Number", "value": None},
            "OEE": {"type": "Number", "value": None}
        }
        cls.blank_throughput = {
            "id": "urn:ngsiv2:i40Asset:Throughput1",
            "type": "i40Asset",
            "i40AssetType": {"type": "Text", "value": "Throughput"},
            "RefWorkstation": {"type": "Relationship", "value": "urn:ngsiv2:i40Asset:Workstation1"},
            "refJob": {"type": "Relationship", "value": "urn:ngsiv2:i40Process:Job202200045"},
            "throughputPerShift": {"type": "Number", "value": None}
        }
        reupload_jsons_to_Orion.main()
        cls.engine = create_engine(
            f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}"
        )
        cls.con = cls.engine.connect()
        if not cls.engine.dialect.has_schema(cls.engine, POSTGRES_SCHEMA):
            cls.engine.execute(sqlalchemy.schema.CreateSchema(POSTGRES_SCHEMA))
        cls.workstation_df = pd.read_csv(os.path.join("csv", WORKSTATION_FILE))
        cls.workstation_df["recvtimets"] = cls.workstation_df["recvtimets"].map(int)
        cls.workstation_df.to_sql(
            name=WORKSTATION_TABLE,
            con=cls.con,
            schema=POSTGRES_SCHEMA,
            index=False,
            dtype=Text,
            if_exists="replace",
        )
        cls.workstation_df = pd.read_sql_query(
            f"select * from {POSTGRES_SCHEMA}.{WORKSTATION_TABLE}", con=cls.con
        )

        cls.job_df = pd.read_csv(os.path.join("csv", JOB_FILE))
        cls.job_df["recvtimets"] = cls.job_df["recvtimets"].map(int)
        cls.job_df.to_sql(
            name=JOB_TABLE,
            con=cls.con,
            schema=POSTGRES_SCHEMA,
            index=False,
            dtype=Text,
            if_exists="replace",
        )
        cls.job_df = pd.read_sql_query(
            f"select * from {POSTGRES_SCHEMA}.{JOB_TABLE}", con=cls.con
        )
        cls.blank_oee = {
                "availability": None,
                "performance": None,
                "quality": None,
                "OEE": None
                }
        cls.correctOEEObject =  copy.deepcopy(cls.blank_oee)
        cls.correctOEEObject["availability"] = 50 / 60
        cls.correctOEEObject["performance"] = (71 * 46) / (50 * 60)
        cls.correctOEEObject["quality"] = 70 / 71
        cls.correctOEEObject["OEE"] = (cls.correctOEEObject["availability"] *
                cls.correctOEEObject["performance"] *
                cls.correctOEEObject["quality"])
        cls.correctThroughPutPerShift = (8 * 3600e3 / 46e3) * 8 * cls.correctOEEObject["OEE"]
        with open(os.path.join("..", "json", "Workstation.json")) as f:
            cls.workstation = json.load(f)

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_get_SLEEP_TIME(self):
        SLEEP_TIME = main.get_SLEEP_TIME()
        self.assertEqual(60, SLEEP_TIME)
        with patch(f"{main.__name__}.os.environ.get") as mock_get:
            string_SLEEP_TIME = "67.88"
            expected = float(string_SLEEP_TIME)
            mock_get.return_value = string_SLEEP_TIME
            self.assertEqual(expected, main.get_SLEEP_TIME())
            with self.assertRaises(ValueError):
                mock_get.return_value = "{'item': 8}"
                main.get_SLEEP_TIME()
            with self.assertRaises(ValueError):
                mock_get.return_value = "value"
                main.get_SLEEP_TIME()

    @patch(f"{OEE.__name__}.datetime", wraps=datetime)
    def test_loop(self, mock_datetime):
        timestamp = datetime.now().timestamp()
        now = datetime(2022, 4, 4, 9, 0, 0)
        mock_datetime.now.return_value = now
        SLEEP_TIME = main.get_SLEEP_TIME()
        main.loop(self.scheduler)
        self.logger.debug(f"test_loop: self.scheduler.queue: {self.scheduler.queue}")
        queue = self.scheduler.queue
        self.assertEqual(len(queue), 1)
        entry = queue[0]
        next_timestamp = entry.time
        interval = next_timestamp - timestamp

        # check if the scheduler's next event is OK
        self.assertTrue(SLEEP_TIME * 0.95 < interval < SLEEP_TIME * 1.05)
        self.assertEqual(entry.priority, 1)
        self.assertEqual(entry.action, main.loop)
        self.assertEqual(entry.argument, (self.scheduler,))
        self.assertEqual(entry.kwargs, {})

        # check calculated KPIs
        downloaded_workstation = remove_orion_metadata(Orion.get(self.workstation["id"]))
        calculated_oee = downloaded_workstation["oeeObject"]["value"]
        calculated_throughputPerShift = downloaded_workstation["throughputPerShift"]["value"]
        self.logger.debug(f"""assert_KPIs_are_correct:
calculated_oee: {calculated_oee}
calculated_throughput: {calculated_throughputPerShift}""")
        assertDeepAlmostEqual(self, self.correctOEEObject, calculated_oee, places=PLACES)
        self.assertAlmostEqual(self.correctOEEObject["availability"], downloaded_workstation["oeeAvailability"]["value"], places=PLACES)
        self.assertAlmostEqual(self.correctOEEObject["performance"], downloaded_workstation["oeePerformance"]["value"], places=PLACES)
        self.assertAlmostEqual(self.correctOEEObject["quality"], downloaded_workstation["oeeQuality"]["value"], places=PLACES)
        self.assertAlmostEqual(self.correctOEEObject["OEE"], downloaded_workstation["OEE"]["value"], places=PLACES)
        assertDeepAlmostEqual(self, self.correctThroughPutPerShift, calculated_throughputPerShift, places=PLACES)

    def test_main(self):
        with patch("sched.scheduler") as mock_scheduler:
            mock_instance_scheduler = Mock()
            mock_scheduler.return_value = mock_instance_scheduler
            main.main()
            mock_instance_scheduler.enter.assert_called_once_with(0, 1, main.loop, (mock_instance_scheduler,))


def test_main():
    unittest.main()


if __name__ == "__main__":
    test_main()
