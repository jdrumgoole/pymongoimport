'''
Created on 7 Aug 2017

@author: jdrumgoole
'''
import unittest
import pymongo
import pymongo.errors

from pymongodbimport.filesplitter import File_Splitter

from pymongodbimport.fileprocessor import FileProcessor

class Test(unittest.TestCase):


    def setUp(self):
        self._client = pymongo.MongoClient( host="mongodb://localhost:27017/TEST_FP")
        self._col = self._client[ "TEST_FP"]["TEST_FP"]

    def tearDown(self):
        self._col.drop()


    def test_fileprocessor(self):
        fp = FileProcessor( self._col, "," )


    def test_property_prices(self):

        start_count = self._col.count()
        fp = FileProcessor( self._col, ','  )
        try:
            fp.processOneFile( "uk_property_prices.csv" )
        except pymongo.errors.BulkWriteError as e :
            print( e )
            raise ;
        lines = File_Splitter.count_lines( "uk_property_prices.csv")
        self.assertEqual( lines, self._col.count() - start_count )

        self.assertTrue( self._col.find_one( { "Postcode" : "NG10 5NN"}) )

    def test_mot_data(self):

        start_count = self._col.count()
        fp=FileProcessor( self._col, '|' )
        fp.processOneFile("10k.txt")
        lines = File_Splitter.count_lines( "10k.txt")
        self.assertEqual(lines, self._col.count() - start_count)
        self.assertTrue( self._col.find_one({"TestID":114624}))

    def test_A_and_E_data(self):
        start_count = self._col.count()
        fp = FileProcessor(self._col, ',')
        fp.processOneFile(input_filename = "AandE_Data_2011-04-10.csv", hasheader=True)
        lines = File_Splitter.count_lines( "AandE_Data_2011-04-10.csv")
        self.assertEqual( lines, self._col.count() - start_count + 1)
        self.assertTrue( self._col.find_one( { "CODE" : "RA4"}) )

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.test_fileprocessor']
    unittest.main()