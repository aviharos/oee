import unittest

from assertDeepAlmostEqual import assertDeepAlmostEqual

PLACES = 4

class test_assertDeepAlmostEqual(unittest.TestCase):
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

    def test_assertDeepAlmostEqual(self):
        dict1 = {"qty1": 1, "item": {"name": "name", "qty": 2}}
        dict2 = {"qty1": 1.000005, "item": {"name": "name", "qty": 2-0.000005}}
        dict3 = {"qty1": 1.005, "item": {"name": "name", "qty": 2-0.000005}}
        assertDeepAlmostEqual(self, dict1, dict2, places=PLACES)
        assertDeepAlmostEqual(self, dict1, dict3, places=PLACES)


def main():
    unittest.main()


if __name__ == "__main__":
    main()
