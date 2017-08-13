'''
Created on 13 Aug 2017

@author: jdrumgoole
'''
import os

class File_Splitter( object ):
    
    def __init__(self, filename, split_count, hasheader=False):
        
        self._filename = filename
        self._split_count = split_count
        self._hasheader = hasheader
        
        self._header_line = ""
        self._output_files = []
        self._size = os.path.getsize( filename )
        self._data_lines = 0
        
    @staticmethod 
    def split_filename( filename, ext ):
        basename = os.path.basename( filename )
        return "%s.%i" % ( basename, ext )

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
    
    def data_lines(self):
        return self._data_lines
    
    def split_file( self, split_size = 0 ):
        
        current_split_size = 0
        output_files = []
        with open( self._filename, "rU") as input_file:
            if self._hasheader :
                self._header_line = input_file.readline()
            line = input_file.readline()
            
            #print( "Creating: '%s'"  % output_files[-1])
            while line != "" :
                self._data_lines = self._data_lines + 1
                if current_split_size == 0 :
                    output_files.append( File_Splitter.split_filename( self._filename, len( output_files ) + 1 ))
                    output_file = open( output_files[ -1 ], "w" )
                    print( "Creating: '%s'"  % output_files[-1])
                    current_split_size = current_split_size + 1
                    output_file.write( line )
                elif current_split_size == split_size :
                    current_split_size = 0
                    output_file.write( line )
                    output_file.close()
                else:
                    current_split_size = current_split_size + 1
                    output_file.write( line )
                    
                line = input_file.readline()
        return output_files
            
    def autosplit( self ):
        
        line_sample = 10
        sample_size = 0
        count = 0
        average_line_size = 0
        
        with open( self._filename, "rU") as f:
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
            
        print( "file size: %i"  % file_size )
        print( "Average line size: %i"  % average_line_size )
        total_lines = file_size / average_line_size
        print( "total lines : %i"  % total_lines )
        split_size = total_lines / self._split_count
        
        print( "Splitting '%s' into at least %i pieces of size %i" %  ( self._filename, self._split_count + 1, split_size ))
        return self.split_file(  split_size ) 
