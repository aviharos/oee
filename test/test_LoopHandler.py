import copy
import json
import os
import sys
import unittest
from unittest.mock import patch

# custom imports
from modules.remove_orion_metadata import remove_orion_metadata 
from modules import reupload_jsons_to_Orion

sys.path.insert(0, os.path.join("..", "app"))
import Orion
from LoopHandler import LoopHandler


class test_LoopHandler(unittest.TestCase):
    pass

    @classmethod
    def setUpClass(cls):
        reupload_jsons_to_Orion.main()

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

    def test_calculate_KPIs(self):
        pass
        # oeeCalculator = OEECalculator(self.ids["ws"])
        # oeeCalculator.prepare(self.con)
        # oee = oeeCalculator.calculate_OEE()
        # throughput = oeeCalculator.calculate_throughput()
        # return oee, throughput

    def test_handle_ws(self):
        pass
        # self.ids = self.blank_ids.copy()
        # self.get_ids(ws)
        # self.logger.info(f'Calculating KPIs for {ws["id"]}')
        # oee, throughput = self.calculate_KPIs()
        # Orion.update((oee, throughput))

    def test_handle(self):
        pass
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
