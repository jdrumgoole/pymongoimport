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
        
        line_count = 0
        total_line_count = 0
        files_list=[]
        for i in files:

            line_count = File_Splitter.count_lines( i )
            #print( "counting %s, %i" % ( i, line_count ))
            total_line_count = total_line_count + line_count
            files_list.append( i )
            
        self.assertTrue( len( files_list ), 3 )
        self.assertEqual( splitter.data_lines_count(), total_line_count )
            
        with open( "AandE_Data_2011-04-10.csv", 'rU') as left_file:
            _ = left_file.readline() # kill header
            for i in files_list:
                with open( i, "rU") as right_file:
                    for right in right_file:
                        left = left_file.readline()
                        self.assertEqual( left, right )

        for i in files_list:
            os.unlink( i )
        
    def test_split_file(self):
        splitter = File_Splitter( "AandE_Data_2011-04-10.csv", split_count=2, hasheader=True )
        
        files = splitter.split_file( split_size = 50 )
        files_list = list( files )


        for i in files_list:
            os.unlink( i )
            
        self.assertEqual( len( files_list), 6 )
        
    def test_split_file_no_header( self ):
        splitter = File_Splitter( "mot_test_set_small.csv", split_count=4, hasheader=False )
        
        line_count = 0
        total_count = 0
        for i in splitter.autosplit():
            line_count = File_Splitter.count_lines( i )
            total_count = total_count + line_count
            
        self.assertEqual( splitter.data_lines_count(), total_count )
        
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.test_autosplit']
    unittest.main()