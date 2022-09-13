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

sys.path.insert(0, os.path.join("..", "app"))
from Logger import getLogger
from LoopHandler import LoopHandler
from object_to_template import object_to_template
import OEE
import Orion

# Load environment variables
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
        with open(os.path.join("..", "json", "Workstation.json")) as f:
            cls.ws = json.load(f)

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        self.loopHandler = LoopHandler()

    def tearDown(self):
        pass

    def test_get_ids(self):
        self.loopHandler.ids = copy.deepcopy(self.loopHandler.blank_ids)
        self.loopHandler.get_ids(self.ws)
        self.assertEqual(self.loopHandler.ids["ws"], self.ws["id"])
        self.assertEqual(self.loopHandler.ids["job"], self.ws["RefJob"]["value"])
        self.assertEqual(self.loopHandler.ids["oee"], self.ws["RefOEE"]["value"])
        self.assertEqual(self.loopHandler.ids["throughput"], self.ws["RefThroughput"]["value"])

        with patch("Orion.exists") as mock_Orion_exist:
            mock_Orion_exist.return_value = False
            with self.assertRaises(ValueError):
                self.loopHandler.get_ids(self.ws)

    @patch(f"{OEE.__name__}.datetime", wraps=datetime)
    def test_calculate_KPIs(self, mock_datetime):
        now = datetime(2022, 4, 4, 9, 0, 0)
        mock_datetime.now.return_value = now
        self.loopHandler.get_ids(self.ws)
        self.loopHandler.con = self.con
        c_oee, c_throughput = self.loopHandler.calculate_KPIs()
        assertDeepAlmostEqual(self, self.oee, c_oee, places=PLACES)
        assertDeepAlmostEqual(self, self.throughput, c_throughput, places=PLACES)

    @patch(f"{OEE.__name__}.datetime", wraps=datetime)
    def test_handle_ws(self, mock_datetime):
        now = datetime(2022, 4, 4, 9, 0, 0)
        mock_datetime.now.return_value = now
        # write false data, LoopHandler should clean it
        self.loopHandler.ids = {"ws": "invalid", "job": "nonexisting", "oee": "urn:ngsi_ld:Throughput:1", "throughput": "urn:ngsi_ld:Part:Core001"}
        workstations = Orion.getWorkstations()
        for item in workstations:
            if item["id"] == "urn:ngsi_ld:Workstation:1":
                ws = item
        self.loopHandler.con = self.con
        self.loopHandler.handle_ws(ws)
        c_oee = remove_orion_metadata(Orion.get("urn:ngsi_ld:OEE:1"))
        c_throughput = remove_orion_metadata(Orion.get("urn:ngsi_ld:Throughput:1"))
        assertDeepAlmostEqual(self, self.oee, c_oee, places=PLACES)
        assertDeepAlmostEqual(self, self.throughput, c_throughput, places=PLACES)

    def test_delete_attributes(self):
        self.loopHandler.get_ids(self.ws)
        self.loopHandler.delete_attributes("OEE")
        downloaded_oee = remove_orion_metadata(Orion.get("urn:ngsi_ld:OEE:1"))
        self.logger.debug(f"delete_attributes: downloaded_oee: {downloaded_oee}")
        self.assertEqual(downloaded_oee, self.blank_oee)

        self.loopHandler.ids["throughput"] = "urn:ngsi_ld:Throughput:1"
        self.loopHandler.delete_attributes("Throughput")
        downloaded_Throughput = remove_orion_metadata(Orion.get("urn:ngsi_ld:Throughput:1"))
        self.assertEqual(downloaded_Throughput, self.blank_throughput)

        with self.assertRaises(NotImplementedError):
            self.loopHandler.delete_attributes("KPI")

        with patch("LoopHandler.object_to_template") as mock_object_to_template:
            mock_object_to_template.side_effect = FileNotFoundError
            with self.assertRaises(FileNotFoundError):
                self.loopHandler.delete_attributes("OEE")

        # with patch("object_to_template.object_to_template") as mock_object_to_template:
        #     mock_object_to_template.return_value = "{'invalid': json}"
        #     with self.assertRaises(json.decoder.JSONDecodeError):
        #         self.loopHandler.delete_attributes("OEE")

    @patch(f"{OEE.__name__}.datetime", wraps=datetime)
    def test_handle(self, mock_datetime):
        now = datetime(2022, 4, 4, 9, 0, 0)
        mock_datetime.now.return_value = now
        reupload_jsons_to_Orion.main()
        self.loopHandler.handle()
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
                # see if the KPIs get deleted in the case of an error
                mock_calculate_KPIs.side_effect = exception
                self.loopHandler.get_ids(self.ws)
                self.loopHandler.handle()
                downloaded_oee = remove_orion_metadata(Orion.get("urn:ngsi_ld:OEE:1"))
                self.assertEqual(downloaded_oee, self.blank_oee)
                downloaded_throughput = remove_orion_metadata(Orion.get("urn:ngsi_ld:Throughput:1"))
                self.assertEqual(downloaded_throughput, self.blank_throughput)
                self.logger.debug(f"handle: {exception}: KPIs cleared")


def main():
    unittest.main()


if __name__ == "__main__":
    main()
