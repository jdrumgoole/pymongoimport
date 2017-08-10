'''
Created on 7 Aug 2017

@author: jdrumgoole
'''
import unittest
import pymongo

from pymongodbimport.fileprocessor import FileProcessor

class Test(unittest.TestCase):


    def setUp(self):
        self._client = pymongo.MongoClient( host="mongodb://localhost:27017/TEST_FP")
        self._col = self._client[ "TEST_FP"]["TEST_FP"]
    def tearDown(self):
        pass

    def test_fileprocessor(self):
        fp = FileProcessor( self._col, ",", "warn", "mongodb", 500 )


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.test_fileprocessor']
    unittest.main()