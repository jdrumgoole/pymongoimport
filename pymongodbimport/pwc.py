'''
Created on 27 Aug 2017

@author: jdrumgoole
'''
import argparse
from pymongodbimport.filesplitter import File_Splitter

if __name__ == "__main__" :
    
    parser = argparse.ArgumentParser()
    parser.add_argument( "filenames", nargs="*", help='list of files')
    args= parser.parse_args()
    
    line_count = 0
    total_count = 0
    for filename in args.filenames:
        line_count = File_Splitter.count_lines( filename )
        total_count = total_count + line_count
        print( "%i\t%s" % ( line_count, filename ))
        
    print( "%i\ttotal" % total_count )
            
        
    
    

    