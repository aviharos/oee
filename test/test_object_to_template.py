# -*- coding: utf-8 -*-
# Standard Library imports
import copy
import json
import os
import sys
import unittest

# Custom imports
sys.path.insert(0, os.path.join("..", "src"))
from object_to_template import object_to_template


class test_object_to_template(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with open(os.path.join("..", "json", "Shift.json")) as f:
            cls.shift = json.load(f)
        with open(os.path.join("..", "json", "Workstation.json")) as f:
            cls.workstation = json.load(f)

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_object_to_template(self):
        shift = copy.deepcopy(self.shift)
        shift["id"] = None
        shift["Start"]["value"] = None
        shift["End"]["value"] = None
        self.assertEqual(
            shift,
            object_to_template(os.path.join("..", "json", "Shift.json")),
        )
        workstation = copy.deepcopy(self.workstation)
        workstation["id"] = None
        workstation["Available"]["value"] = None
        workstation["RefJob"]["value"] = None
        workstation["RefOEE"]["value"] = None
        workstation["RefThroughput"]["value"] = None
        workstation["RefShift"]["value"] = None
        self.assertEqual(
            workstation, object_to_template(os.path.join("..", "json", "Workstation.json"))
        )


def main():
    unittest.main()


if __name__ == "__main__":
    main()
