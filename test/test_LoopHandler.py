"""
Throughout this file, setattr is used to extend the unit tests with parametric
tests.
"""
# Standard Library imports
import copy
from datetime import datetime
import json
from logging import getLevelName
import os
import sys
import unittest
from unittest.mock import patch

# PyPI imports
import pandas as pd
import numpy as np
import psycopg2
import sqlalchemy
from sqlalchemy import create_engine, delete
from sqlalchemy.types import Text
# custom imports
from modules.remove_orion_metadata import remove_orion_metadata
from modules import reupload_jsons_to_Orion
from modules.assertDeepAlmostEqual import assertDeepAlmostEqual

sys.path.insert(0, os.path.join("..", "src"))
from Logger import getLogger
from LoopHandler import LoopHandler
import OEE
import Orion

# Load environment variables
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

def test_generator_check_throughput(right_throughput, calculated_throughput):
    def test(self):
        self.assertEqual(right_throughput["id"], calculated_throughput["id"])
        self.assertEqual(right_throughput["RefWorkstation"], calculated_throughput["RefWorkstation"])
        self.assertEqual(right_throughput["RefJob"], calculated_throughput["RefJob"])
        self.assertAlmostEqual(right_throughput["ThroughputPerShift"]["value"], calculated_throughput["ThroughputPerShift"]["value"], places=PLACES)
    return test


class test_LoopHandler(unittest.TestCase):
    logger = getLogger(__name__)

    @classmethod
    def setUpClass(cls):
        """
        Almost all of this setUpClass is identical to the test_OEECalculator object's setUpClass
        """
        cls.maxDiff = None
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
                "Availability": None,
                "Performance": None,
                "Quality": None,
                "OEE": None
                }
        cls.correctOEEObject =  copy.deepcopy(cls.blank_oee)
        cls.correctOEEObject["Availability"] = 50 / 60
        cls.correctOEEObject["Performance"] = (71 * 46) / (50 * 60)
        cls.correctOEEObject["Quality"] = 70 / 71
        cls.correctOEEObject["OEE"] = (cls.correctOEEObject["Availability"] *
                cls.correctOEEObject["Performance"] *
                cls.correctOEEObject["Quality"])
        cls.correctThroughPutPerShift = (8 * 3600e3 / 46e3) * 8 * cls.correctOEEObject["OEE"]
        with open(os.path.join("..", "json", "Workstation.json")) as f:
            cls.workstation = json.load(f)

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        self.loopHandler = LoopHandler()

    def tearDown(self):
        pass

    @patch(f"{OEE.__name__}.datetime", wraps=datetime)
    def test_calculate_KPIs(self, mock_datetime):
        now = datetime(2022, 4, 4, 9, 0, 0)
        mock_datetime.now.return_value = now
        self.loopHandler.con = self.con
        calculated_oee, calculated_throughputPerShift = self.loopHandler.calculate_KPIs(self.workstation["id"])
        assertDeepAlmostEqual(self, self.correctOEEObject, calculated_oee, places=PLACES)
        assertDeepAlmostEqual(self, self.correctThroughPutPerShift, calculated_throughputPerShift, places=PLACES)

    def assert_KPIs_are_cleared(self):
        downloaded_workstation = remove_orion_metadata(Orion.get(self.workstation["id"]))
        self.logger.debug(f"test_clear_all_KPIs: downloaded_workstation: {downloaded_workstation}")
        self.assertEqual(downloaded_workstation["oeeObject"]["value"], self.blank_oee)
        self.assertEqual(downloaded_workstation["ThroughputPerShift"]["value"], None)
        self.assertEqual(downloaded_workstation["oeeAvailability"]["value"], None)
        self.assertEqual(downloaded_workstation["oeePerformance"]["value"], None)
        self.assertEqual(downloaded_workstation["oeeQuality"]["value"], None)
        self.assertEqual(downloaded_workstation["OEE"]["value"], None)

    def assert_KPIs_are_correct(self):
        downloaded_workstation = remove_orion_metadata(Orion.get(self.workstation["id"]))
        calculated_oee = downloaded_workstation["oeeObject"]["value"]
        calculated_throughputPerShift = downloaded_workstation["ThroughputPerShift"]["value"]
        assertDeepAlmostEqual(self, self.correctOEEObject, calculated_oee, places=PLACES)
        self.assertAlmostEqual(self.correctOEEObject["Availability"], downloaded_workstation["oeeAvailability"]["value"], places=PLACES)
        self.assertAlmostEqual(self.correctOEEObject["Performance"], downloaded_workstation["oeePerformance"]["value"], places=PLACES)
        self.assertAlmostEqual(self.correctOEEObject["Quality"], downloaded_workstation["oeeQuality"]["value"], places=PLACES)
        self.assertAlmostEqual(self.correctOEEObject["OEE"], downloaded_workstation["OEE"]["value"], places=PLACES)
        assertDeepAlmostEqual(self, self.correctThroughPutPerShift, calculated_throughputPerShift, places=PLACES)

    def write_values_into_KPIs(self):
        workstation = copy.deepcopy(self.workstation)
        workstation["oeeObject"]["value"] = self.correctOEEObject
        workstation["oeeAvailability"]["value"] = self.correctOEEObject["Availability"]
        workstation["oeePerformance"]["value"] = self.correctOEEObject["Performance"]
        workstation["oeeQuality"]["value"] = self.correctOEEObject["Quality"]
        workstation["OEE"]["value"] = self.correctOEEObject["OEE"]
        workstation["ThroughputPerShift"]["value"] = self.correctThroughPutPerShift
        Orion.update([workstation])

    @patch(f"{OEE.__name__}.datetime", wraps=datetime)
    def test_handle_workstation(self, mock_datetime):
        now = datetime(2022, 4, 4, 9, 0, 0)
        mock_datetime.now.return_value = now
        self.loopHandler.con = self.con
        self.loopHandler.handle_workstation(self.workstation["id"])
        self.assert_KPIs_are_correct()

        for exception in (
            AttributeError,
            KeyError,
            RuntimeError,
            TypeError,
            ValueError,
            ZeroDivisionError,
            psycopg2.OperationalError,
            sqlalchemy.exc.OperationalError,
            ):
            with patch("LoopHandler.LoopHandler.calculate_KPIs") as mock_calculate_KPIs:
                self.logger.debug(f"test_clear_all_KPIs: exception: {exception}")
                self.write_values_into_KPIs()
                self.assert_KPIs_are_correct()
                mock_calculate_KPIs.side_effect = exception
                self.loopHandler.handle()
                self.assert_KPIs_are_cleared()
                self.logger.debug(f"handle: {exception}: KPIs cleared")

    def test_clear_all_KPIs(self):
        self.write_values_into_KPIs()
        self.assert_KPIs_are_correct()
        self.loopHandler.workstations = [copy.deepcopy(self.workstation)]
        self.loopHandler.clear_all_KPIs()
        self.assert_KPIs_are_cleared()

    @patch(f"{OEE.__name__}.datetime", wraps=datetime)
    def test_handle(self, mock_datetime):
        now = datetime(2022, 4, 4, 9, 0, 0)
        mock_datetime.now.return_value = now
        reupload_jsons_to_Orion.main()
        self.loopHandler.handle()
        workstations = Orion.getWorkstations()
        self.assertEqual(len(workstations), 1)
        self.assert_KPIs_are_correct()


def main():
    unittest.main()


if __name__ == "__main__":
    main()
