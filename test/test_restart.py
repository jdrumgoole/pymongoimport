'''
Created on 31 Jul 2017

@author: jdrumgoole
'''
import unittest
import pymongo
from pymongodbimport.restart import Restarter
from pymongodbimport.bulkwriter import BulkWriter
class Test(unittest.TestCase):


    def setUp(self):
        client = pymongo.MongoClient()
        self._db = client[ "RESTART_TEST"]
        self._collection = self._db[ "output"]

    def tearDown(self):
        self._db.drop_collection( "restartLog")


    def test_Restart(self):
        r = Restarter( self._db, self._collection, input_filename="test.csv", batch_size=50 )
        bw - 

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.test_Restart']
    unittest.main()