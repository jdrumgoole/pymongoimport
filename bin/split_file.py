'''
Created on 11 Aug 2017

@author: jdrumgoole
'''
import argparse
import sys
import os

from pymongodbimport.filesplitter import File_Splitter

# class File_Splitter_x( object ):
#     
#     def __init__(self, filename, split_count, hasheader=False):
#         
#         self._filename = filename
#         self._split_count = split_count
#         self._hasheader = hasheader
#         
#         self._header_line = ""
#         self._output_files = []
#         self._size = os.path.getsize( filename )
#         
#     @staticmethod 
#     def split_filename( filename, ext ):
#         basename = os.path.basename( filename )
#         return "%s.%i" % ( basename, ext )
# 
#     def size(self):
#         return self._size
#     
#     def no_header_size(self):
#         return self._size - len( self._header_line )
#     
#     def output_files(self):
#         return self._output_files
#     
#     def split_file( self, split_size = 0 ):
#         
#         current_split_size = 0
#         output_files = []
#         with open( self._filename, "rU") as input_file:
#             if self._hasheader :
#                 self._header_line = input_file.readline()
#             line = input_file.readline()
#             
#             #print( "Creating: '%s'"  % output_files[-1])
#             while line != "" :
#                 if current_split_size == 0 :
#                     output_files.append( File_Splitter_x.split_filename( self._filename, len( output_files ) + 1 ))
#                     output_file = open( output_files[ -1 ], "w" )
#                     print( "Creating: '%s'"  % output_files[-1])
#                     current_split_size = current_split_size + 1
#                     output_file.write( line )
#                 elif current_split_size == split_size :
#                     current_split_size = 0
#                     output_file.write( line )
#                     output_file.close()
#                 else:
#                     current_split_size = current_split_size + 1
#                     output_file.write( line )
# 
#                 line = input_file.readline()
#              
#         return output_files
#             
#     def autosplit( self, splits ):
#         
#         line_sample = 10
#         sample_size = 0
#         count = 0
#         average_line_size = 0
#         
#         with open( self._filename, "rU") as f:
#             if self._hasheader:
#                 self._header_line = f.readline()
#                 
#             line = f.readline()
#             while line and count < 10 :
#                 sample_size = sample_size + len( line )
#                 count = count + 1
#                 line = f.readline()
#                 
#             average_line_size = sample_size / line_sample
#             
#         file_size = self._size
#     
#         if self._hasheader:
#             file_size = file_size - len( self._header_line )
#             
#         print( "file size: %i"  % file_size )
#         print( "Average line size: %i"  % average_line_size )
#         total_lines = file_size / average_line_size
#         print( "total lines : %i"  % total_lines )
#         split_size = total_lines / splits
#         
#         print( "Splitting file into %i pieces of size %i" %  ( splits, split_size ))
#         return self.split_file(  split_size ) 


if __name__ == '__main__':
    
    __VERSION__ = "0.1"
    
    usage_message = '''
    
    split a text file into seperate pieces. if you specify autosplit then the program
    will use the first ten lines to calcuate an average line size and use that to determine
    the rough number of splits.
    
    if you use --splitsize then the file will be split using --splitsize chunks until it is consumed
    '''
    
    parser = argparse.ArgumentParser( usage=usage_message, version=__VERSION__ )
    
    parser.add_argument( "--autosplit", type=int, 
                         help="split file based on loooking at the first ten lines and overall file size [default : %(default)s]")
    parser.add_argument('--hasheader',  default=False, action="store_true", help="Use header line for column names [default: %(default)s]")
    parser.add_argument( "--splitsize", type=int, help="Split file into chunks of this size")
    parser.add_argument( "filenames", nargs="*", help='list of files')
    args= parser.parse_args( sys.argv[1:])
    
    print( "Splitting file")
    if len( args.filenames ) == 0 :
        print( "No input file specified to split")
    elif len( args.filenames) > 1 :
        print( "More than one input file specified ( %s ) only splitting the first file:'%s'" % 
               ( " ".join( args.filenames ), args.filenames[ 0 ] ))
    
    splitter = File_Splitter( args.filenames[ 0 ], args.autosplit, args.hasheader )
    if args.autosplit:
        print( "Autosplitting: '%s" % args.filenames[ 0 ] )
        files = splitter.autosplit()
    else:
        print( "Splitting '%s' using %i splitsize" % ( args.filenames[ 0 ], args.splitsize ))
        files = splitter.split_file( args.splitsize )
    #print( "Split '%s' into %i parts"  % ( args.filenames[ 0 ], len( files )))
    count = 1
    total_size = 0
    for i in files:
        size = os.path.getsize( i )
        total_size = total_size + size
        print ( "%i. %s : size: %i" % ( count, i, size ))
        count = count + 1
    
    if total_size != splitter.no_header_size():
        raise ValueError( "Filesize of original and pieces does not match: total_size: %i, no header split_size: %i" % ( total_size, splitter.no_header_size()))