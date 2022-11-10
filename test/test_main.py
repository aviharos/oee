# -*- coding: utf-8 -*-
# Standard Library imports
from datetime import datetime
import copy
from logging import getLoggerClass
import os
import sched
import sys
import time
import unittest
from unittest.mock import patch, Mock

# PyPI imports
import numpy as np
import pandas as pd
import psycopg2
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.types import Text

# Custom imports
sys.path.insert(0, os.path.join("..", "src"))
from modules.remove_orion_metadata import remove_orion_metadata
from modules import reupload_jsons_to_Orion
from modules.assertDeepAlmostEqual import assertDeepAlmostEqual
from object_to_template import object_to_template
from Logger import getLogger
import main
from LoopHandler import LoopHandler
import OEE
import Orion

POSTGRES_HOST = os.environ.get("POSTGRES_HOST")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT")
POSTGRES_USER = os.environ.get("POSTGRES_USER")
POSTGRES_SCHEMA = os.environ.get("POSTGRES_SCHEMA")

# Constants
PLACES = 4
WS_FILE = "urn_ngsi_ld_workstation_1_workstation.csv"
WS_TABLE = "urn_ngsi_ld_workstation_1_workstation"
JOB_FILE = "urn_ngsi_ld_job_202200045_job.csv"
JOB_TABLE = "urn_ngsi_ld_job_202200045_job"


class test_object_to_template(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """
        Almost all of this setUpClass is identical to the test_OEECalculator object's setUpClass
        """
        cls.scheduler = sched.scheduler(time.time, time.sleep)
        cls.logger = getLogger(__name__)
        cls.blank_oee = {
            "type": "OEE",
            "id": "urn:ngsi_ld:OEE:1",
            "RefWorkstation": {"type": "Relationship", "value": "urn:ngsi_ld:Workstation:1"},
            "RefJob": {"type": "Relationship", "value": "urn:ngsi_ld:Job:202200045"},
            "Availability": {"type": "Number", "value": None},
            "Performance": {"type": "Number", "value": None},
            "Quality": {"type": "Number", "value": None},
            "OEE": {"type": "Number", "value": None}
        }
        cls.blank_throughput = {
            "type": "Throughput",
            "id": "urn:ngsi_ld:Throughput:1",
            "RefWorkstation": {"type": "Relationship", "value": "urn:ngsi_ld:Workstation:1"},
            "RefJob": {"type": "Relationship", "value": "urn:ngsi_ld:Job:202200045"},
            "ThroughputPerShift": {"type": "Number", "value": None}
        }
        reupload_jsons_to_Orion.main()
        cls.engine = create_engine(
            f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}"
        )
        cls.con = cls.engine.connect()
        if not cls.engine.dialect.has_schema(cls.engine, POSTGRES_SCHEMA):
            cls.engine.execute(sqlalchemy.schema.CreateSchema(POSTGRES_SCHEMA))
        cls.ws_df = pd.read_csv(os.path.join("csv", WS_FILE))
        cls.ws_df["recvtimets"] = cls.ws_df["recvtimets"].map(int)
        cls.ws_df.to_sql(
            name=WS_TABLE,
            con=cls.con,
            schema=POSTGRES_SCHEMA,
            index=False,
            dtype=Text,
            if_exists="replace",
        )
        cls.ws_df = pd.read_sql_query(
            f"select * from {POSTGRES_SCHEMA}.{WS_TABLE}", con=cls.con
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
        cls.oee = object_to_template(os.path.join("..", "json", "OEE.json"))
        cls.oee["id"] = "urn:ngsi_ld:OEE:1"
        cls.oee["RefWorkstation"] = {"type": "Relationship", "value": "urn:ngsi_ld:Workstation:1"}
        cls.oee["RefJob"] = {"type": "Relationship", "value": "urn:ngsi_ld:Job:202200045"}
        cls.oee["Availability"]["value"] = 50 / 60
        cls.oee["Quality"]["value"] = 70 / 71
        cls.oee["Performance"]["value"] = (71 * 46) / (50 * 60)
        cls.oee["OEE"]["value"] = cls.oee["Availability"]["value"] * cls.oee["Performance"]["value"] * cls.oee["Quality"]["value"]
        cls.throughput = object_to_template(os.path.join("..", "json", "Throughput.json"))
        cls.throughput["id"] = "urn:ngsi_ld:Throughput:1"
        cls.throughput["RefWorkstation"] = {"type": "Relationship", "value": "urn:ngsi_ld:Workstation:1"}
        cls.throughput["RefJob"] = {"type": "Relationship", "value": "urn:ngsi_ld:Job:202200045"}
        cls.throughput["ThroughputPerShift"]["value"] = (8 * 3600e3 / 46e3) * 8 * cls.oee["OEE"]["value"]

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
        ORION_HOST = os.environ.get('ORION_HOST')
        ORION_PORT = os.environ.get('ORION_PORT')
        url = f"http://{ORION_HOST}:{ORION_PORT}/v2/entities?type=OEE"
        _, oees = Orion.getRequest(url)
        self.assertEqual(len(oees), 1)
        c_oee = remove_orion_metadata(oees[0])
        assertDeepAlmostEqual(self, self.oee, c_oee, places=PLACES)
        url = f"http://{ORION_HOST}:{ORION_PORT}/v2/entities?type=Throughput"
        _, throughputs = Orion.getRequest(url)
        self.assertEqual(len(throughputs), 1)
        c_throughput = remove_orion_metadata(throughputs[0])
        assertDeepAlmostEqual(self, self.throughput, c_throughput, places=PLACES)

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
