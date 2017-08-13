'''
Created on 13 Aug 2017

@author: jdrumgoole
'''
import unittest
import os

from pymongodbimport.filesplitter import File_Splitter
class Test(unittest.TestCase):

    def test_autosplit(self):
        
        splitter = File_Splitter( "AandE_Data_2011-04-10.csv", split_count=2, hasheader=True )
        files = splitter.autosplit()
        self.assertTrue( len( files), 3 )
        
        line_count = 0
        for i in files:
            line_count = line_count + File_Splitter.count_lines( i )
            
        self.assertEqual( splitter.data_lines(), line_count )
            
        with open( "AandE_Data_2011-04-10.csv", 'rU') as left_file:
            _ = left_file.readline() # kill header
            for i in files:
                with open( i, "rU") as right_file:
                    for right in right_file:
                        left = left_file.readline()
                        self.assertEqual( left, right )

        for i in files:
            os.unlink( i )
        
    def test_split_file(self):
        splitter = File_Splitter( "AandE_Data_2011-04-10.csv", split_count=2, hasheader=True )
        
        files = splitter.split_file( split_size = 50 )
        self.assertEqual( len( files ), 6 )

        for i in files:
            os.unlink( i )
            
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.test_autosplit']
    unittest.main()