'''
Created on 27 Aug 2017

@author: jdrumgoole
'''
import unittest
from pymongodbimport.split_file import split_file
from pymongodbimport.filesplitter import File_Splitter

import os

class Test_split_file(unittest.TestCase):


    def setUp(self):
        pass


    def tearDown(self):
        pass


    def _compare_input_output(self, input_filename, output_filenames ):
        original_count = 0
        file_piece_count = 0
        with  open( input_filename, "rU") as original_file :
            for filename in File_Splitter.shim_names( output_filenames ):
                with open( filename, "rU") as file_piece:
                    for line in file_piece:
                        left = original_file.readline()
                        original_count = original_count + 1 
                        right = line
                        file_piece_count = file_piece_count + 1 
                        self.assertEqual( left, right )
                os.unlink( filename )
                
    def test_auto_split(self):
        input_filename = "mot_test_set_small.csv"
        filenames = split_file( [ "--autosplit", "2", input_filename ])
        self._compare_input_output(input_filename,  filenames )
        
    def test_split_size(self):
        input_filename = "mot_test_set_small.csv"
        filenames = split_file( [ "--splitsize", "50", input_filename ])
        self._compare_input_output(input_filename,  filenames )
        filenames = split_file( [ "--splitsize", "1", input_filename ])
        self._compare_input_output(input_filename,  filenames )
        filenames = split_file( [ "--splitsize", "23", input_filename ])
        self._compare_input_output(input_filename,  filenames )
        
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()