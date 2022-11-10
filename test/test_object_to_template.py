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
        with open(os.path.join("..", "json", "OperatorSchedule.json")) as f:
            cls.op_sch = json.load(f)
        with open(os.path.join("..", "json", "Workstation.json")) as f:
            cls.ws = json.load(f)

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_object_to_template(self):
        op_sch = copy.deepcopy(self.op_sch)
        op_sch["id"] = None
        op_sch["OperatorWorkingScheduleStartsAt"]["value"] = None
        op_sch["OperatorWorkingScheduleStopsAt"]["value"] = None
        self.assertEqual(
            op_sch,
            object_to_template(os.path.join("..", "json", "OperatorSchedule.json")),
        )
        ws = copy.deepcopy(self.ws)
        ws["id"] = None
        ws["Available"]["value"] = None
        ws["RefJob"]["value"] = None
        ws["RefOEE"]["value"] = None
        ws["RefThroughput"]["value"] = None
        ws["RefOperatorSchedule"]["value"] = None
        self.assertEqual(
            ws, object_to_template(os.path.join("..", "json", "Workstation.json"))
        )


def main():
    unittest.main()


if __name__ == "__main__":
    main()
