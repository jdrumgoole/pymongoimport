'''
Created on 8 Aug 2017

@author: jdrumgoole
'''
import unittest
import os

import pymongo

from pymongodbimport.fieldconfig import FieldConfig
from pymongodbimport.bulkwriter import BulkWriter

class Test(unittest.TestCase):


    def setUp(self):
        self._client = pymongo.MongoClient( host="mongodb://localhost:27017")
        self._db = self._client[ "FC_TEST"]
        self._col = self._db[ "FC_TEST"]

    def tearDown(self):
        pass


    def test_FieldConfig(self, filename = "test_fieldconfig.ff"):
        fc = FieldConfig( filename )
        
        self.assertEqual( len( fc.fields()), 4 )
        
        self.assertEqual( fc.fields()[0], "Test 1")
        self.assertEqual( fc.fields()[3], "Test 4")
        
    def test_generate_fieldfile(self):
        
        try :
            fc_filename = FieldConfig.generate_field_filename( 'inventory.csv' )
            self.assertEqual( fc_filename, "inventory.ff")
            fc_filename = FieldConfig.generate_field_filename( 'inventory.csv', ext="xx" )
            self.assertEqual( fc_filename, "inventory.xx")
            fc_filename = FieldConfig.generate_field_filename( 'inventory.csv', ext=".xx" )
            self.assertEqual( fc_filename, "inventory.xx")
            
            fc_filename = FieldConfig.generate_field_filename( 'inventory.csv' )
            self.assertEqual( fc_filename, "inventory.ff")
            
            self.assertFalse( os.path.isfile( "inventory.testff"))
            fc_filename = FieldConfig.generate_field_file( "inventory.csv", ext="testff" )
            self.assertTrue( os.path.isfile( "inventory.testff"))
            fc = FieldConfig( fc_filename, hasheader=True)
            
            writer = BulkWriter( self._col, fc )
            writer.insert_file( "inventory.csv" )
#             with open( "inventory.csv", "rU" ) as f :
#                 reader = fc.get_dict_reader( f )
#                 print( "Inventory")
#                 for i in reader:
#                     print( i )
                
        except AssertionError :
            if os.path.isfile( "inventory.testff") :
                os.unlink( "inventory.testff" )
        

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.test_FieldConfig']
    unittest.main()