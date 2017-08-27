'''
Created on 13 Aug 2017

@author: jdrumgoole
'''
import unittest
import os

from pymongodbimport.filesplitter import File_Splitter

class Test(unittest.TestCase):

    def test_count_lines(self):
        self.assertEqual( 3, File_Splitter.count_lines( "threelines.txt" ))
        self.assertEqual( 0, File_Splitter.count_lines( "emptyfile.txt" ))
        self.assertEqual( 4, File_Splitter.count_lines( "fourlines.txt" ))
        
    def test_split_file(self):
        splitter = File_Splitter( "fourlines.txt", hasheader=False )
        line_count = File_Splitter.count_lines( "fourlines.txt" )
        split_lines = 3
        split_remainder = line_count % split_lines

        #print( "%s has %i lines" % ( "fourlines.txt", line_count ))
        files = list( splitter.split_file( split_lines ))
        last_file = files[-1][0]
        other_files = files[0:-1]
        for ( i, _ ) in other_files:
            #print( "xxx : '%s'" % i )
            self.assertEqual( File_Splitter.count_lines( i ), split_lines )
            os.unlink( i )
        if split_remainder > 0 :
            #print( "yyy: '%s'" % last_file )
            self.assertEqual( os.path.getsize( last_file ), split_remainder )
            os.unlink( last_file )
            
    def test_autosplit_file(self):
        splitter = File_Splitter( __file__, hasheader=False )
        files = list( splitter.autosplit( 2 ))
        self.assertEqual( len( files ), 1 )
            
    def test_get_average_line_size(self):
        self.assertEqual( 10, File_Splitter.get_average_line_size( "tenlines.txt" ))
        
    def test_autosplit_csv(self):

        splitter = File_Splitter( "AandE_Data_2011-04-10.csv", hasheader=True )
        files = splitter.autosplit( 2 )
        
        line_count = 0
        total_line_count = 0
        files_list=[]
        
        for i in files:
            #print( " XXX : %s %s" % ( __name__, i[0]))
            line_count = File_Splitter.count_lines( i[0] )
            #print( "counting %s, %i" % ( i, line_count ))
            total_line_count = total_line_count + line_count
            files_list.append( i[0] )
            
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
        
    def test_split_file_manual_split(self):
        splitter = File_Splitter( "AandE_Data_2011-04-10.csv", hasheader=True )
        
        files = splitter.split_file( split_size = 50 )
        files_list = list( files )

        for ( i, _ ) in files_list:
            os.unlink( i )
            
        self.assertEqual( len( files_list), 5 )
        
    def test_split_empty_file(self):
        splitter = File_Splitter( "emptyfile.txt", hasheader=False )
        files = list( splitter.split_file(  10 ))
        self.assertEqual( len( files ), 0 )
        splitter = File_Splitter( "emptyfile.txt", hasheader=False )
        files = list( splitter.autosplit(  10 ))
        
    def _clean_files( self, filenames ):
        
        for i in filenames:
            self.assertTrue( os.path.isfile( i ))
            os.unlink( i )
            self.assertFalse( os.path.isfile( i ))
            
    def _split_file(self, filename,splitter ):
        original_count = 0
        file_piece_count = 0
        
        with  open( filename, "rU") as original_file :
            if splitter.has_header() :
                _ = original_file.readline()
            for filename in splitter.autosplit( 4 ):
                with open( filename[0], "rU") as file_piece:
                    for line in file_piece:
                        left = original_file.readline()
                        original_count = original_count + 1 
                        right = line
                        file_piece_count = file_piece_count + 1 
                        self.assertEqual( left, right )
                os.unlink( filename[0] )
                        
            
        self.assertEqual( splitter.data_lines_count(), original_count )
        
    def test_split_file_no_header( self ):
        filename = "mot_test_set_small.csv"
        splitter = File_Splitter( filename, hasheader=False )
        self._split_file( filename, splitter )
        
        filename = "AandE_Data_2011-04-10.csv"
        splitter = File_Splitter( filename, hasheader=True )
        self._split_file( filename, splitter )
                
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.test_autosplit']
    unittest.main()