# -*- coding: utf-8 -*-
# Standard Library imports
import copy
import json
import os
import requests
import sys
import unittest
from unittest.mock import patch

# Custom imports
sys.path.insert(0, os.path.join("..", "src"))
import Orion
from modules.remove_orion_metadata import remove_orion_metadata

ORION_HOST = os.environ.get("ORION_HOST")
ORION_PORT = os.environ.get("ORION_PORT")

orion_entities = f"http://{ORION_HOST}:{ORION_PORT}/v2/entities"

PLACES = 5


class test_Orion(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """ Upload Core001 and Workstation to Orion """
        cls.maxDiff = None
        with open(os.path.join("..", "json", "Core001.json"), "r") as f:
            cls.obj = json.load(f)
        requests.post(url=orion_entities, json=cls.obj)
        with open(os.path.join("..", "json", "Workstation.json"), "r") as f:
            cls.workstation1 = json.load(f)
        # make a second Workstation
        cls.workstation2 = copy.deepcopy(cls.workstation1)
        cls.workstation2["id"] = "urn:ngsiv2:i40Asset:Workstation2"
        cls.workstation2["RefJob"]["value"] = "urn:ngsiv2:i40Process:Job202200045_mod"

    @classmethod
    def tearDownClass(cls):
        # delete uploaded Workstation objects
        requests.delete(url=f'{orion_entities}/{cls.workstation1["id"]}')
        requests.delete(url=f'{orion_entities}/{cls.workstation2["id"]}')

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_getRequest(self):
        # try downloading the Core001 object
        status_code, downloaded_json = Orion.getRequest(
            f'{orion_entities}/{self.obj["id"]}'
        )
        downloaded_json = remove_orion_metadata(downloaded_json)
        self.assertEqual(status_code, 200)
        self.assertEqual(downloaded_json, self.obj)

        with patch("requests.get") as mocked_get:
            mocked_get.side_effect = ValueError
            with self.assertRaises(RuntimeError):
                Orion.getRequest(f'{orion_entities}/{self.obj["id"]}')

    def test_get(self):
        self.assertEqual(
            remove_orion_metadata(Orion.get("urn:ngsiv2:i40Asset:Part_Core001")), self.obj
        )
        with patch("requests.get") as mocked_get:
            mocked_get.status_code = 201
            with self.assertRaises(RuntimeError):
                Orion.get("urn:ngsiv2:i40Asset:Part_Core001")

    def test_exists(self):
        self.assertTrue(Orion.exists("urn:ngsiv2:i40Asset:Part_Core001"))
        self.assertFalse(Orion.exists("urn:ngsiv2:i40Asset:Part_Core002"))

    def test_getWorkstations(self):
        # delete uploaded Workstation objects
        requests.delete(url=f'{orion_entities}/{self.workstation1["id"]}')
        requests.delete(url=f'{orion_entities}/{self.workstation2["id"]}')

        # there are no Workstation objects in Orion
        self.assertEqual(len(Orion.getWorkstations()), 0)

        # post 2 Workstation objects
        requests.post(url=orion_entities, json=self.workstation1)
        requests.post(url=orion_entities, json=self.workstation2)
        downloaded_workstations = [
            remove_orion_metadata(workstation) for workstation in Orion.getWorkstations()
        ]
        self.assertEqual(len(downloaded_workstations), 2)

        # check if the 2 downloaded Workstation objects match the uploaded ones
        if downloaded_workstations[0]["id"] == self.workstation1["id"]:
            self.assertEqual(downloaded_workstations[0], self.workstation1)
            self.assertEqual(downloaded_workstations[1], self.workstation2)
        else:
            self.assertEqual(downloaded_workstations[0], self.workstation2)
            self.assertEqual(downloaded_workstations[1], self.workstation1)

    def test_update(self):
        # create copies of Workstation objects to be used in the test's scope
        workstation1m = self.workstation1.copy()
        workstation1m["RefJob"]["value"] = "urn:ngsiv2:i40Process:Job202200045_mod"
        workstation2m = self.workstation2.copy()
        workstation2m["RefOEE"]["value"] = "urn:ngsiv2:i40Asset:OEE3"

        # update both Workstations in Orion
        requests.post(url=orion_entities, json=workstation1m)
        requests.post(url=orion_entities, json=workstation2m)
        Orion.update([workstation1m, workstation2m])

        # check if update was successful
        downloaded_workstations = [
            remove_orion_metadata(workstation) for workstation in Orion.getWorkstations()
        ]
        self.assertEqual(len(downloaded_workstations), 2)
        if downloaded_workstations[0]["id"] == workstation1m["id"]:
            self.assertEqual(downloaded_workstations[0], workstation1m)
            self.assertEqual(downloaded_workstations[1], workstation2m)
        else:
            self.assertEqual(downloaded_workstations[0], workstation2m)
            self.assertEqual(downloaded_workstations[1], workstation1m)

    def test_update_attribute(self):
        id = self.workstation1["id"]
        # Boolean
        workstation = self.workstation1.copy()
        requests.post(url=orion_entities, json=workstation)
        workstation["Available"]["value"] = False
        Orion.update_attribute(id, "Available", "Boolean", False)
        downloaded = remove_orion_metadata(Orion.get(id))
        self.assertEqual(downloaded, workstation)

        # Number
        workstation = self.workstation1.copy()
        requests.post(url=orion_entities, json=workstation)
        workstation["ThroughputPerShift"]["value"] = 8
        Orion.update_attribute(id, "ThroughputPerShift", "Number", 8)
        downloaded = remove_orion_metadata(Orion.get(id))
        self.assertEqual(downloaded, workstation)
        
        # json
        workstation = self.workstation1.copy()
        requests.post(url=orion_entities, json=workstation)
        payload = {
            "OEE": 0.9 * 0.9 * 0.9,
            "Availability": 0.9,
            "Performance": 0.9,
            "Quality": 0.9 
        }
        workstation["OEEObject"]["value"] = payload
        Orion.update_attribute(id, "OEEObject", "OEE", payload)
        downloaded = remove_orion_metadata(Orion.get(id))
        self.assertAlmostEqual(downloaded["OEEObject"]["value"]["Availability"], workstation["OEEObject"]["value"]["Availability"], places=PLACES)
        self.assertAlmostEqual(downloaded["OEEObject"]["value"]["Performance"], workstation["OEEObject"]["value"]["Performance"], places=PLACES)
        self.assertAlmostEqual(downloaded["OEEObject"]["value"]["Quality"], workstation["OEEObject"]["value"]["Quality"], places=PLACES)
        self.assertAlmostEqual(downloaded["OEEObject"]["value"]["OEE"], workstation["OEEObject"]["value"]["OEE"], places=PLACES)


def main():
    unittest.main()


if __name__ == "__main__":
    main()
