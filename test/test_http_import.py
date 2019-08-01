import unittest
import os

import pymongo
import requests

from pymongoimport.configfile import ConfigFile
from pymongoimport.csvparser import CSVParser
from pymongoimport.filereader import FileReader
from pymongoimport.filewriter import FileWriter

path_dir = os.path.dirname(os.path.realpath(__file__))

def check_internet():
    url='http://www.google.com/'
    timeout=2
    try:
        r = requests.get(url, timeout=timeout)
        r.raise_for_status()
        return True
    except requests.ConnectionError:
        pass
    return False

def f(path):
    return os.path.join(path_dir, path)

class TestHTTPImport(unittest.TestCase):

    def setUp(self):
        self._client = pymongo.MongoClient()
        self._db = self._client[ "PYIM_HTTP_TEST"]
        self._collection = self._db["PYIM_HTTP_TEST"]
        self._cfg = ConfigFile(f("data/2018_Yellow_Taxi_Trip_Data_1000.ff"))
        self._parser = CSVParser(self._cfg, has_header=True, delimiter=";")

    def tearDown(self):
        self._db.drop_collection("PYIM_HTTP_TEST")

    def test_limit(self):
        #
        # need to test limit with a noheader file
        #

        reader = FileReader(f("data/2018_Yellow_Taxi_Trip_Data_1000.csv"),
                            parser=self._parser)
        count = 0
        for doc in reader.read_file(limit=10):
            count = count + 1

        self.assertEqual(count, 10)

    def test_local_import(self):
        reader = FileReader(f("data/2018_Yellow_Taxi_Trip_Data_1000.csv"),
                            parser=self._parser)

        before_doc_count = self._collection.count_documents({})

        writer = FileWriter(self._collection, reader)
        writer.write(10)

        after_doc_count = self._collection.count_documents({})

        self.assertEqual( after_doc_count - before_doc_count, 10)

    def test_http_import(self):
        if check_internet():
            csv_parser = CSVParser(self._cfg, has_header=True, delimiter=";")
            reader = FileReader("https://data.cityofnewyork.us/api/views/biws-g3hs/rows.csv?accessType=DOWNLOAD&bom=true&format=true&delimiter=%3B",
                                csv_parser)

            writer = FileWriter(self._collection, reader)
            before_doc_count = self._collection.count_documents({})
            after_doc_count = writer.write(1000)
            self.assertEqual(after_doc_count - before_doc_count, 1000)
        else:
            print("Warning: No internet: test_http_import() skipped")




if __name__ == '__main__':
    unittest.main()
