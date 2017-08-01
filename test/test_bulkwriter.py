'''
Created on 26 Jul 2017

@author: jdrumgoole
'''
import unittest
import pymongo
from pymongodbimport.bulkwriter import BulkWriter

class Test(unittest.TestCase):


    def setUp(self):
        client = pymongo.MongoClient()
        self._db = self._client[ "test"]
        self._collection = self._db[ 'test']


    def tearDown(self):
        self._db.drop_collection( "test")


    def testBulkWriter(self):
        d = { "a" : "b"}
        


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testBulkWriter']
    unittest.main()