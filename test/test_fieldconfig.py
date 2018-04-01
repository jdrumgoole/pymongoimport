'''
Created on 8 Aug 2017

@author: jdrumgoole
'''
import unittest
import os
import datetime

import pymongo

from pymongodbimport.fieldconfig import FieldConfig, FieldConfigException
from pymongodbimport.bulkwriter import BulkWriter
from pymongodbimport.logger import Logger
from pymongodbimport.filesplitter import File_Splitter

class Test(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(Test, self).__init__(*args, **kwargs)
        Logger.add_null_hander()

    def setUp(self):
        self._client = pymongo.MongoClient( host="mongodb://localhost:27017")
        self._db = self._client[ "FC_TEST"]
        self._col = self._db[ "FC_TEST"]

    def tearDown(self):
        #self._db.drop_collection( "FC_TEST")
        pass

    def test_FieldConfig(self ):
        fc = FieldConfig( "test_fieldconfig.ff" )
        
        self.assertEqual( len( fc.fields()), 4 )
        
        self.assertEqual( fc.fields()[0], "Test 1")
        self.assertEqual( fc.fields()[3], "Test 4")

        fc = FieldConfig( "uk_property_prices.ff")

        self.assertEqual(len( fc.fields()), 15)

        self.assertEqual( fc.fields()[0], "txn")
        self.assertEqual( fc.fields()[2], "Date of Transfer" )
        self.assertEqual( fc.fields()[14], "PPD Category Type")

    def test_delimiter_no_header(self):
        start_count = self._col.count()
        fc = FieldConfig( "10k.ff", delimiter='|', hasheader=False )
        bw = BulkWriter( self._col, fc )
        bw.insert_file( "10k.txt" )
        self.assertEqual( self._col.count() - start_count, 10000 )

        
    def test_delimiter_header(self):
        start_count = self._col.count()
        fc = FieldConfig( "AandE_Data_2011-04-10.ff", delimiter=',', hasheader=True )
        bw = BulkWriter( self._col, fc )
        bw.insert_file( "AandE_Data_2011-04-10.csv")
        self.assertEqual( self._col.count() - start_count, 300 )
        
    def test_generate_field_filename(self):
        fc_filename = FieldConfig.generate_field_filename( 'inventory.csv' )
        self.assertEqual( fc_filename, "inventory.ff")
        fc_filename = FieldConfig.generate_field_filename( 'inventory.csv', ext="xx" )
        self.assertEqual( fc_filename, "inventory.xx")
        fc_filename = FieldConfig.generate_field_filename( 'inventory.csv', ext=".xx" )
        self.assertEqual( fc_filename, "inventory.xx")
        fc_filename = FieldConfig.generate_field_filename( 'inventory.csv' )
        self.assertEqual( fc_filename, "inventory.ff")
        
    def test_dict_reader(self):
        fc_filename = FieldConfig.generate_field_file( "inventory.csv" )
        fc = FieldConfig( fc_filename )
        with open( "inventory.csv", "rU")  as f :
            if fc.hasheader():
                _ = f.readline()
            reader = fc.get_dict_reader( f )
            for row in reader:
                for field in fc.fields():
                    self.assertTrue( field in row )


        fc = FieldConfig( "uk_property_prices.ff")
        with open( "uk_property_prices.csv", "rU")  as f :
            if fc.hasheader():
                _ = f.readline()
            reader = fc.get_dict_reader( f )
            for row in reader:
                for field in fc.fields():
                    self.assertTrue( field in row )
                    self.assertTrue(type(row["Price"]) == str)
                    self.assertTrue(type(row["Date of Transfer"]) == str )
                
    def test_generate_fieldfile(self):
        
        fc_filename = FieldConfig.generate_field_file( "inventory.csv", ext="testff" )
        self.assertTrue( os.path.isfile( "inventory.testff"))
        fc = FieldConfig( fc_filename, hasheader=True)

        start_count = self._col.count()
        writer = BulkWriter( self._col, fc )
        writer.insert_file( "inventory.csv" )
        line_count = File_Splitter.count_lines( "inventory.csv")
        self.assertEqual(self._col.count() - start_count, line_count -1) # header must be subtracted

        os.unlink( "inventory.testff")
        
        with open( "inventory.csv", "rU")  as f :
            if fc.hasheader():
                _ = f.readline()
            reader = fc.get_dict_reader( f )
            fields = fc.fields()
            for row in reader:
                #print( row )
                for f in fields:
                    row[ f ] = fc.type_convert( row[ f ], fc.typeData( f )) # remember we type convert fields

                doc = self._col.find_one( row )
                self.assertTrue(  doc )
                    
            
    def test_date(self):
        fc = FieldConfig( "inventory_dates.ff", hasheader=True)

        start_count = self._col.count()
        writer = BulkWriter( self._col, fc )
        writer.insert_file( "inventory.csv" )
        line_count = File_Splitter.count_lines( "inventory.csv")
        self.assertEqual( self._col.count() - start_count, line_count -1 ) # header must be subtracted


        with open( "inventory.csv", "rU")  as f :
            if fc.hasheader():
                _ = f.readline()
            reader = fc.get_dict_reader( f )
            fields = fc.fields()
            for row in reader:
                #print( row )
                for f in fields:
                    row[ f ]= fc.type_convert( row[ f ], fc.typeData( f )) # remember we type convert fields

                doc = self._col.find_one(row)
                self.assertTrue( doc  )
        
    def test_field_config_exception(self):

        #f.open( "duplicateID.ff" )
        self.assertRaises( FieldConfigException, FieldConfig, "nosuchfile.ff" )
        #print( "fields: %s" % f.fields())

    def testFieldDict(self):
        f = FieldConfig( "testresults.ff", delimiter="|")
        d = f.fieldDict()
        self.assertTrue( d.has_key("TestID"))
        self.assertTrue( d.has_key("FirstUseDate"))
        self.assertTrue( d.has_key( "Colour"))
        self.assertTrue( d["TestID"]["type"] == "int")
        
    def test_duplicate_id(self):
        self.assertRaises( FieldConfigException,FieldConfig, "duplicate_id.ff" )
        
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.test_FieldConfig']
    unittest.main()