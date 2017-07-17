'''
Created on 3 Mar 2016

@author: jdrumgoole
'''
import unittest

from pymongodbimport.fieldconfig import FieldConfig

class Test(unittest.TestCase):


    def setUp(self):
        pass


    def tearDown(self):
        pass


    def testDuplicateID(self):
        f = FieldConfig()

        #f.open( "duplicateID.ff" )
        self.assertRaises( ValueError, f.read, "duplicateID.ff" )
        #print( "fields: %s" % f.fields())

    def testFieldDict(self):
        f = FieldConfig( "testresults.ff")
        d = f.fieldDict()
        self.assertTrue( d.has_key("TestID"))
        self.assertTrue( d.has_key("FirstUseDate"))
        self.assertTrue( d.has_key( "Colour"))
        self.assertTrue( d[ "TestID"]["id"] == "True", d[ "TestID"]["id"]  )
        self.assertTrue( d["TestID"]["type"] == "int")
        
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()