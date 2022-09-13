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
sys.path.insert(0, os.path.join("..", "app"))
import Orion
from modules.remove_orion_metadata import remove_orion_metadata

ORION_HOST = os.environ.get("ORION_HOST")
ORION_PORT = os.environ.get("ORION_PORT")

orion_entities = f"http://{ORION_HOST}:{ORION_PORT}/v2/entities"


class test_Orion(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """ Upload Core001 and Workstation to Orion """
        with open(os.path.join("..", "json", "Core001.json"), "r") as f:
            cls.obj = json.load(f)
        requests.post(url=orion_entities, json=cls.obj)
        with open(os.path.join("..", "json", "Workstation.json"), "r") as f:
            cls.ws1 = json.load(f)
        # make a second Workstation
        cls.ws2 = copy.deepcopy(cls.ws1.copy)
        cls.ws2["id"] = "urn:ngsi_ld:Workstation:2"
        cls.ws2["RefJob"]["value"] = "urn:ngsi_ld:Job:2000000"

    @classmethod
    def tearDownClass(cls):
        pass

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
            remove_orion_metadata(Orion.get("urn:ngsi_ld:Part:Core001")), self.obj
        )
        with patch("requests.get") as mocked_get:
            mocked_get.status_code = 201
            with self.assertRaises(RuntimeError):
                Orion.get("urn:ngsi_ld:Part:Core001")

    def test_exists(self):
        self.assertTrue(Orion.exists("urn:ngsi_ld:Part:Core001"))
        self.assertFalse(Orion.exists("urn:ngsi_ld:Part:Core123"))

    def test_getWorkstations(self):
        # delete uploaded Workstation objects
        requests.delete(url=f'{orion_entities}/{self.ws1["id"]}')
        requests.delete(url=f'{orion_entities}/{self.ws2["id"]}')

        # there are no Workstation objects in Orion
        self.assertEqual(len(Orion.getWorkstations()), 0)

        # post 2 Workstation objects
        requests.post(url=orion_entities, json=self.ws1)
        requests.post(url=orion_entities, json=self.ws2)
        downloaded_workstations = [
            remove_orion_metadata(ws) for ws in Orion.getWorkstations()
        ]
        self.assertEqual(len(downloaded_workstations), 2)

        # check if the 2 downloaded Workstation objects match the uploaded ones
        if downloaded_workstations[0]["id"] == self.ws1["id"]:
            self.assertEqual(downloaded_workstations[0], self.ws1)
            self.assertEqual(downloaded_workstations[1], self.ws2)
        else:
            self.assertEqual(downloaded_workstations[0], self.ws2)
            self.assertEqual(downloaded_workstations[1], self.ws1)

    def test_update(self):
        # create copies of Workstation objects to be used in the test's scope
        ws1m = self.ws1.copy()
        ws1m["RefJob"]["value"] = "urn:ngsi_ld:Job:12"
        ws2m = self.ws2.copy()
        ws2m["RefOEE"]["value"] = "urn:ngsi_ld:OEE:3"

        # update both Workstations in Orion
        requests.post(url=orion_entities, json=ws1m)
        requests.post(url=orion_entities, json=ws2m)
        Orion.update([ws1m, ws2m])

        # check if update was successful
        downloaded_workstations = [
            remove_orion_metadata(ws) for ws in Orion.getWorkstations()
        ]
        self.assertEqual(len(downloaded_workstations), 2)
        if downloaded_workstations[0]["id"] == ws1m["id"]:
            self.assertEqual(downloaded_workstations[0], ws1m)
            self.assertEqual(downloaded_workstations[1], ws2m)
        else:
            self.assertEqual(downloaded_workstations[0], ws2m)
            self.assertEqual(downloaded_workstations[1], ws1m)


def main():
    unittest.main()


if __name__ == "__main__":
    main()
