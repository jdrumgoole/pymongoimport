'''
Created on 13 Aug 2017

@author: jdrumgoole
'''
import os
from collections import OrderedDict
class File_Splitter( object ):
    
    def __init__(self, input_filename, split_count, hasheader=False):
        
        self._input_filename = input_filename
        self._split_count = split_count
        self._hasheader = hasheader
        self._files = OrderedDict()
        self._header_line = ""
        self._output_files = []
        self._size = os.path.getsize( self._input_filename )
        self._data_lines_count = 0
        
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
            for _ in input_file:
                count = count + 1   
        return count
    
    def no_header_size(self):
        return self._size - len( self._header_line )
    
    def output_files(self):
        return self._output_files
    
    def data_lines_count(self):
        return self._data_lines_count
    
    def split_file( self, split_size = 0 ):
        
        current_split_size = 0
        output_files = []
        with open( self._input_filename, "rU") as input_file:
            if self._hasheader :
                self._header_line = input_file.readline()
            line = input_file.readline()
            
            #print( "Creating: '%s'"  % output_files[-1])
            file_count = 0
            while line != "" :
                #print( output_files )
                self._data_lines_count = self._data_lines_count + 1
                if current_split_size == 0 :
                    file_count = file_count + 1
                    ( output_file, filename ) = self.new_file( self._input_filename, file_count )
                    #print( "Creating: '%s'"  % filename )
                    current_split_size = current_split_size + 1
                    output_file.write( line )
                    self._files[ filename ] = self._files[ filename ] + len( line )
                elif current_split_size == split_size :
                    current_split_size = 0
                    output_file.write( line )
                    self._files[ filename ] = self._files[ filename ] + len( line )
                    output_file.close()
                    
                    #print( "yield at split: %s" % output_files[ -1 ] )
                    yield filename
                else:
                    current_split_size = current_split_size + 1
                    output_file.write( line )
                    self._files[ filename ] = self._files[ filename ] + len( line )
                    
                line = input_file.readline()
            
        if not output_file.close:
            output_file.close()
           
        if current_split_size < split_size : 
            #print( "yield at final close: %s" % output_files[ -1 ] )
            if not output_file.closed :
                output_file.close()  
            yield filename
            
    def autosplit( self ):
        
        average_line_size = 0
        line_sample = 10
        sample_size = 0
        count = 0
        
        with open( self._input_filename, "rU") as f:
            if self._hasheader:
                self._header_line = f.readline()
                
            line = f.readline()
            while line and count < 10 :
                sample_size = sample_size + len( line )
                count = count + 1
                line = f.readline()
                
            average_line_size = sample_size / line_sample
            
        file_size = self._size
    
        if self._hasheader:
            file_size = file_size - len( self._header_line )
            
        #print( "file size: %i"  % file_size )
        #print( "Average line size: %i"  % average_line_size )
        total_lines = file_size / average_line_size
        #print( "total lines : %i"  % total_lines )
        split_size = total_lines / self._split_count
        
        #print( "Splitting '%s' into at least %i pieces of size %i" %  ( self._input_filename, self._split_count + 1, split_size ))
        for i in self.split_file(  split_size ):
            yield i
