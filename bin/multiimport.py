'''
Created on 11 Aug 2017

@author: jdrumgoole
'''
import argparse
import sys
from multiprocessing import Process
from collections import OrderedDict
import time

from pymongodbimport.filesplitter import File_Splitter
from pymongodbimport.main import mainline

def main_line( *a ):
    print( " ".join( a ))
    mainline( a )
  

def strip_arg( args, remove_arg, has_trailing=False ):
    try :
        location = args.index( remove_arg )
        if has_trailing:
            del args[ location + 1 ]
        del args[ location]
        
    except ValueError :
        pass
    
    return args
            
             
            
from pymongodbimport.argparser import pymongodb_arg_parser
if __name__ == '__main__':
    
    __VERSION__ = "0.1"
    
    usage_message = '''
    
    A master script to manage uploading of a single data file as multiple input files. Multi-import
    will optionally split a single file (specified by the --single argument) or optionally upload an
    already split list of files passed in on the command line.
    Each file is uplaoded by a separate pymongoimport subprocess. 
    '''
    
    parser = argparse.ArgumentParser( parents=pymongodb_arg_parser(),usage=usage_message, version=__VERSION__ )
    parser.add_argument( "--autosplit", type=int, 
                         help="split file based on loooking at the first ten lines and overall file size [default : %(default)s]")
    parser.add_argument( "--splitsize", type=int, help="Split file into chunks of this size")   
    args= parser.parse_args( sys.argv[1:])
    
    new_args = sys.argv[1:]
    children = OrderedDict()
    if args.autosplit:
        print( "Autosplitting file")
        if len( args.filenames ) == 0 :
            print( "No input file specified to split")
        elif len( args.filenames) > 1 :
            print( "More than one input file specified ( %s ) only splitting the first file:'%s'" % 
                   ( " ".join( args.filenames ), args.filenames[ 0 ] ))
        new_args = strip_arg( new_args, "--autosplit", True)
    else:
        new_args = strip_arg( new_args, "--splitsize", True )
        
    for i in args.filenames:
        new_args = strip_arg( new_args,  i )
        
    splitter = File_Splitter( args.filenames[ 0 ], args.autosplit, args.hasheader )
    process_count = 0
    
    stripped_args = []
    if args.autosplit:
        generator = splitter.autosplit()
        
        
    else:
        generator = splitter.split_file( args.splitsize )
        
    start = time.time()
    
    for filename in generator:
        print( "Processing '%s'" % filename )
        process_count = process_count + 1
        proc_name = "%s.%i" % ( "main_line", process_count )
        new_args.append( filename )
        proc = Process( target=main_line, name=proc_name, args=new_args )
        children[ proc_name ] = proc
        print( "starting sub process: %s" % proc_name )
        proc.start()
        
    for i in children.keys():
        print( "Waiting for process: '%s' to complete" % i )
        children[ i ].join()
        
    finish = time.time()
    
    print( "Total elapsed time:%f" % ( finish - start ))