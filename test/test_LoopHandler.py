# Standard Library imports
import copy
from datetime import datetime
import json
import os
import sys
import unittest
from unittest.mock import patch

# PyPI imports
import pandas as pd
import numpy as np
import psycopg2
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.types import Text
# custom imports
from modules.remove_orion_metadata import remove_orion_metadata
from modules import reupload_jsons_to_Orion

sys.path.insert(0, os.path.join("..", "app"))
import OEE
import Orion
from LoopHandler import LoopHandler
from object_to_template import object_to_template

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


class test_LoopHandler(unittest.TestCase):
    pass

    @classmethod
    def setUpClass(cls):
        """
        Almost all of this setUpClass is identical to the test_OEECalculator object's setUpClass
        """
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
        self.loopHandler = LoopHandler()
        self.loopHandler.ids["OEE"] = "urn:ngsi_ld:OEE:1"
        self.loopHandler.ids["Throughput"] = "urn:ngsi_ld:Throughput:1"
        self.loopHandler.ids["ws"] = "urn:ngsi_ld:Workstation:1"
        self.loopHandler.ids["job"] = "urn:ngsi_ld:Job:202200045"

    def tearDown(self):
        pass

    def test_get_ids(self):
        self.loopHandler.ids = copy.deepcopy(self.loopHandler.blank_ids)
        with open(os.path.join("..", "json", "Workstation.json")) as f:
            ws = json.load(f)
        self.loopHandler.get_ids(ws)
        self.assertEqual(self.loopHandler.ids["ws"], ws["id"])
        self.assertEqual(self.loopHandler.ids["job"], ws["RefJob"]["value"])
        self.assertEqual(self.loopHandler.ids["oee"], ws["RefOEE"]["value"])
        self.assertEqual(self.loopHandler.ids["throughput"], ws["RefThroughput"]["value"])

        with patch("Orion.exists") as mock_Orion_exist:
            mock_Orion_exist.return_value = False
            with self.assertRaises(ValueError):
                self.loopHandler.get_ids(ws)

    @patch(f"{OEE.__name__}.datetime", wraps=datetime)
    def test_calculate_KPIs(self, mock_datetime):
        now = datetime(2022, 4, 4, 9, 0, 0)
        mock_datetime.now.return_value = now
        self.loopHandler.con = self.con
        c_oee, c_throughput = self.loopHandler.calculate_KPIs()
        self.assertEqual(self.oee["id"], c_oee["id"])
        self.assertEqual(self.oee["RefWorkstation"], c_oee["RefWorkstation"])
        self.assertEqual(self.oee["RefJob"], c_oee["RefJob"])
        self.assertAlmostEqual(self.oee["Availability"]["value"], c_oee["Availability"]["value"], places=PLACES)
        self.assertAlmostEqual(self.oee["Performance"]["value"], c_oee["Performance"]["value"], places=PLACES)
        self.assertAlmostEqual(self.oee["Quality"]["value"], c_oee["Quality"]["value"], places=PLACES)
        self.assertEqual(self.throughput["id"], c_throughput["id"])
        self.assertEqual(self.throughput["RefWorkstation"], c_throughput["RefWorkstation"])
        self.assertEqual(self.throughput["RefJob"], c_throughput["RefJob"])
        self.assertAlmostEqual(self.throughput["ThroughputPerShift"]["value"], c_throughput["ThroughputPerShift"]["value"], places=PLACES)

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
        self.assertEqual(self.oee["id"], c_oee["id"])
        self.assertEqual(self.oee["RefWorkstation"], c_oee["RefWorkstation"])
        self.assertEqual(self.oee["RefJob"], c_oee["RefJob"])
        self.assertAlmostEqual(self.oee["Availability"]["value"], c_oee["Availability"]["value"], places=PLACES)
        self.assertAlmostEqual(self.oee["Performance"]["value"], c_oee["Performance"]["value"], places=PLACES)
        self.assertAlmostEqual(self.oee["Quality"]["value"], c_oee["Quality"]["value"], places=PLACES)
        self.assertEqual(self.throughput["id"], c_throughput["id"])
        self.assertEqual(self.throughput["RefWorkstation"], c_throughput["RefWorkstation"])
        self.assertEqual(self.throughput["RefJob"], c_throughput["RefJob"])
        self.assertAlmostEqual(self.throughput["ThroughputPerShift"]["value"], c_throughput["ThroughputPerShift"]["value"], places=PLACES)

    def test_delete_attributes(self):
        self.loopHandler.ids["oee"] = "urn:ngsi_ld:OEE:1"
        self.loopHandler.delete_attributes("OEE")
        blank_OEE = {
            "type": "OEE",
            "id": "urn:ngsi_ld:OEE:1",
            "RefWorkstation": {"type": "Relationship", "value": "urn:ngsi_ld:Workstation:1"},
            "RefJob": {"type": "Relationship", "value": "urn:ngsi_ld:Job:202200045"},
            "Availability": {"type": "Number", "value": None},
            "Performance": {"type": "Number", "value": None},
            "Quality": {"type": "Number", "value": None},
            "OEE": {"type": "Number", "value": None}
        }
        downloaded_OEE = remove_orion_metadata(Orion.get("urn:ngsi_ld:OEE:1"))
        self.assertEqual(downloaded_OEE, blank_OEE)

        self.loopHandler.ids["throughput"] = "urn:ngsi_ld:Throughput:1"
        self.loopHandler.delete_attributes("Throughput")
        downloaded_Throughput = remove_orion_metadata(Orion.get("urn:ngsi_ld:Throughput:1"))
        blank_Throughput = {
            "type": "Throughput",
            "id": "urn:ngsi_ld:Throughput:1",
            "RefWorkstation": {"type": "Relationship", "value": "urn:ngsi_ld:Workstation:1"},
            "RefJob": {"type": "Relationship", "value": "urn:ngsi_ld:Job:202200045"},
            "ThroughputPerShift": {"type": "Number", "value": None}
        }
        self.assertEqual(downloaded_Throughput, blank_Throughput)

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
        url = f"http://{ORION_HOST}:{ORION_PORT}/v2/entities?type=Throughput"
        _, throughputs = Orion.getRequest(url)
        self.assertEqual(len(throughputs), 1)
        c_throughput = remove_orion_metadata(throughputs[0])
        self.assertEqual(self.oee["id"], c_oee["id"])
        self.assertEqual(self.oee["RefWorkstation"], c_oee["RefWorkstation"])
        self.assertEqual(self.oee["RefJob"], c_oee["RefJob"])
        self.assertAlmostEqual(self.oee["Availability"]["value"], c_oee["Availability"]["value"], places=PLACES)
        self.assertAlmostEqual(self.oee["Performance"]["value"], c_oee["Performance"]["value"], places=PLACES)
        self.assertAlmostEqual(self.oee["Quality"]["value"], c_oee["Quality"]["value"], places=PLACES)
        self.assertEqual(self.throughput["id"], c_throughput["id"])
        self.assertEqual(self.throughput["RefWorkstation"], c_throughput["RefWorkstation"])
        self.assertEqual(self.throughput["RefJob"], c_throughput["RefJob"])
        self.assertAlmostEqual(self.throughput["ThroughputPerShift"]["value"], c_throughput["ThroughputPerShift"]["value"], places=PLACES)
        # self.engine = create_engine(
        #     f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}"
        # )
        # try:
        #     with self.engine.connect() as self.con:
        #         self.workstations = Orion.getWorkstations()
        #         if len(self.workstations) == 0:
        #             self.logger.critical(
        #                 "No Workstation is found in the Orion broker, no OEE data"
        #             )
        #         for ws in self.workstations:
        #             self.handle_ws(ws)
        #
        # except (
        #     AttributeError,
        #     KeyError,
        #     RuntimeError,
        #     TypeError,
        #     ValueError,
        #     ZeroDivisionError,
        #     psycopg2.OperationalError,
        #     sqlalchemy.exc.OperationalError,
        # ) as error:
        #     # could not calculate OEE or Throughput
        #     # try to delete the OEE and Throughput values, if we have enough data
        #     self.logger.error(error)
        #     if None in self.ids.values():
        #         self.logger.critical(
        #             "A critical error occured, not even the ids of the objects could be determined. No OEE data. An OEE and a Throughput object should be cleared, but it cannot be determined, which ones."
        #         )
        #     else:
        #         self.logger.warning(
        #             "An error happened, trying to clear all attributes of the OEE and Throughput objects."
        #         )
        #         for object_ in ("OEE", "Throughput"):
        #             self.delete_attributes(object_)
        #         self.logger.warning(
        #             "Cleared OEE and Throughput."
        #         )
        # finally:
        #     self.engine.dispose()


def main():
    unittest.main()


if __name__ == "__main__":
    main()
