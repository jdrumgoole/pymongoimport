import unittest
import os

import pymongo

from pymongoimport.pymongoimport_main import pymongoimport_main

path_dir = os.path.dirname(os.path.realpath(__file__))
def f(path):
    return os.path.join(path_dir, path)

class MyTestCase(unittest.TestCase):

    def setUp(self) -> None:
        self._client = pymongo.MongoClient()
        self._db = self._client["test"]

    def test_main(self):
        collection = self._db["inventory"]
        self._db.drop_collection("inventory")
        pymongoimport_main(["--database", "test",
                            "--loglevel", "CRITICAL", # suppress output for test
                            "--hasheader",
                            "--collection", "inventory",
                            f("data/inventory.csv")])

        results = list(collection.find())
        self.assertEqual(len(results), 4)

    def tearDown(self) -> None:
        collection = self._db["inventory"]
        self._db.drop_collection("inventory")

if __name__ == '__main__':
    unittest.main()
