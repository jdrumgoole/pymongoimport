'''
Created on 13 Aug 2017

@author: jdrumgoole

File Splitter is a class that takes a file and splits it into seperate pieces. Its purpose built for
use wit pymongodbimport and is expected to be used to split CSV files (which may or may not have
a header, hence the **hasheader** argument). When splitting a file the output files are produced without
a header file. 

The file can be split by number of lines using the **split_file** function. Alternatively 
the file may be split automatically into a number of pieces specified by as a parameter to
**autosplit**. Autosplitting is achieved by by guessing the average line size by looking at
the first ten lines and taking an average of those lines.

The output files have the same name as the input file with a number appended ( .1, .2, .3 etc.).

There is also a **count_lines** function to count the lines in a file.

'''
import os
from collections import OrderedDict
class File_Splitter( object ):
    
    def __init__(self, input_filename, hasheader=False):
        
        self._input_filename = input_filename
        self._hasheader = hasheader
        self._files = OrderedDict()
        self._header_line = ""
        self._size = os.path.getsize( self._input_filename )
        self._data_lines_count = 0
        self._size_threshold = 1024 * 10
        
    def new_file( self, filename, ext ):
        basename = os.path.basename( filename )
        filename = "%s.%i" % ( basename, ext )
        self._files[ filename ] = 0
        newfile = open( filename, "w" )
        return ( newfile, filename )

    def size(self):
        return self._size
    
    @staticmethod
    def count_lines( filename ):
        count = 0
        with open( filename, "rU")  as input_file :
            for ( count, _ ) in enumerate( input_file, 1) :
                #print( "%i\t%s" % ( count,  l.rstrip()))
                pass
        return count
    
    def has_header(self):
        return self._hasheader
    
    def no_header_size(self):
        return self._size - len( self._header_line )
    
    def output_files(self):
        return self._files.keys()
    
    def data_lines_count(self):
        return self._data_lines_count
    
    def split_file( self, split_size = 0 ):
        
        current_split_size = 0
        file_count = 0
        filename = None
        output_file = None
        
        with open( self._input_filename, "rU") as input_file:
            if self._hasheader :
                self._header_line = input_file.readline()
                
            line = input_file.readline()

            first_iteration = True
            while line != "" :
                #print( output_files )
                if first_iteration :
                    file_count = file_count + 1
                    ( output_file, filename ) = self.new_file( self._input_filename, file_count )
                    output_file.write( line )
                    #output_file.flush()
                    self._data_lines_count = self._data_lines_count + 1
                    current_split_size = current_split_size + 1
                    self._files[ filename ] = self._files[ filename ] + len( line )
                    first_iteration = False
                    
                elif current_split_size == split_size :

                    self._data_lines_count = self._data_lines_count + 1
                    current_split_size = 0
                    self._files[ filename ] = self._files[ filename ] + len( line )
                    output_file.close()
                    print( "file chunk: %s %i" % ( filename, split_size ))
                    yield ( filename, split_size )
                    file_count = file_count + 1
                    ( output_file, filename ) = self.new_file( self._input_filename, file_count )
                    output_file.write( line )
                    #output_file.flush()
                    current_split_size = current_split_size + 1
                    self._files[ filename ] = self._files[ filename ] + len( line )
                    #print( "yield at split: %s" % output_files[ -1 ] )

                else:
                    output_file.write( line )
                    #output_file.flush()
                    self._data_lines_count = self._data_lines_count + 1
                    current_split_size = current_split_size + 1
                    self._files[ filename ] = self._files[ filename ] + len( line )

                line = input_file.readline()
           
        if current_split_size > 0 and current_split_size  <= split_size : 
            output_file.close()
            yield ( filename, current_split_size )
           
    @staticmethod
    def get_average_line_size( filename ):
        
        sample_size = 0
        line_sample = 10
        count = 0
        
        with open( filename, "rU") as f:
            line = f.readline()
            sample_size = sample_size + len( line )
            while line and count < line_sample :
                count = count + 1
                line = f.readline()
                sample_size = sample_size + len( line )
            
        if count > 0 :    
            return sample_size / count
        else:
            return 0
    
    @staticmethod
    def shim_names( g ):
        for i in g :
            yield i[0]
            
    def autosplit( self, split_count ):
                
        average_line_size = File_Splitter.get_average_line_size( self._input_filename )
            
        if average_line_size != 0 :
            file_size = self._size
    
            total_lines = file_size / average_line_size
            #print( "total lines : %i"  % total_lines )
            
            split_size = total_lines / split_count
            
            #print( "Splitting '%s' into at least %i pieces of size %i" %  ( self._input_filename, self._split_count + 1, split_size ))
            for i in self.split_file(  split_size ):
                yield i
