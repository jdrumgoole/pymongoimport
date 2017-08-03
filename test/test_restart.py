'''
Created on 31 Jul 2017

@author: jdrumgoole
'''
import unittest
import pymongo
from pymongodbimport.restart import Restarter
from pymongodbimport.bulkwriter import BulkWriter
from pymongodbimport.fieldconfig import FieldConfig
class Test(unittest.TestCase):


    def setUp(self):
        client = pymongo.MongoClient()
        self._db = client[ "RESTART_TEST"]
        self._collection = self._db[ "output"]

    def tearDown(self):
        self._db.drop_collection( "restartLog")


    def test_Restart(self):
        chunk_size = 500
        r = Restarter( self._db, input_filename="10k.txt",  batch_size=chunk_size )
        fc = FieldConfig( "10k.ff" )
        bw = BulkWriter( self._collection, fc, hasheader=False, chunksize = chunk_size )
        bw.bulkWrite( "100k.txt")
        bw.insert_file( "100k.txt")
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.test_Restart']
    unittest.main()