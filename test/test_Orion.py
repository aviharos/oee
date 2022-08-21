# -*- coding: utf-8 -*-
# Standard Library imports
import json
import os
import requests
import sys
import unittest
from unittest.mock import patch, Mock

# Custom imports
sys.path.insert(0, os.path.join('..', 'app'))
from modules.remove_orion_metadata import remove_orion_metadata
import Orion

orion_entities = 'http://localhost:1026/v2/entities'

class testOrion(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_getRequest(self):
        with open(os.path.join('..', 'json', 'Core001.json'), 'r') as f:
            json_ = json.load(f)
        requests.post(url=orion_entities, json=json_)
        status_code, downloaded_json = Orion.getRequest(f'{orion_entities}/{json_["id"]}')
        downloaded_json = remove_orion_metadata(downloaded_json)
        self.assertEqual(status_code, 200)
        self.assertEqual(downloaded_json, json_)
        with patch('requests.get') as mocked_get:
            mocked_get.side_effect = ValueError
            with self.assertRaises(RuntimeError):
                Orion.getRequest(f'{orion_entities}/{json_["id"]}')
        # with patch('requests.get') as mocked_get:
        #     mock = Mock()
        #     mocked_json = Mock(side_effect=requests.exceptions.JSONDecodeError)
        #     mock.json = mocked_json
        #     mocked_get.return_value = mock
        #     with self.assertRaises(ValueError):
        #         Orion.getRequest(f'{orion_entities}/{json_["id"]}')
        

def main():
    ans = input('''The testing process needs MOMAMS up and running on localhost.
Please start it if you have not already.
Also, the tests delete and create objects in the Orion broker.
It also changes the PostgreSQL data.
Never use the tests on a production environment.
Do you still want to proceed? [yN]''')
    if ans != 'y':
        print('exiting...')
        sys.exit(0)
    try:
        unittest.main()
    except Exception as error:
        print(error)

if __name__ == '__main__':
    main()

