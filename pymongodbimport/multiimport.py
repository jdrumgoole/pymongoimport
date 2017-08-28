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
from pymongodbimport.mongoimport import mongo_import
from pymongodbimport.logger import Logger
from pymongodbimport.argparser import add_standard_args

  
def strip_arg( arg_list, remove_arg, has_trailing=False ):
    '''
    Remove arg and arg argument from a list of args. If has_trailing is true then
    remove --arg value else just remove --arg.
    
    Args:
    
    arg_list (list) : List of args that we want to remove items from
    remove_arg (str) : Name of arg to remove. Must match element in `arg_list`.
    has_trailing (boolean) : If the arg in `remove_arg` has an arg. Then make sure
    to remove that arg as well
    '''
    try :
        location = arg_list.index( remove_arg )
        if has_trailing:
            del arg_list[ location + 1 ]
        del arg_list[ location]
        
    except ValueError :
        pass
    
    return arg_list       

def multi_import( *argv ):

    
    __VERSION__ = "0.1"
    
    usage_message = '''
    
    A master script to manage uploading of a single data file as multiple input files. Multi-import
    will optionally split a single file (specified by the --single argument) or optionally upload an
    already split list of files passed in on the command line.
    Each file is uplaoded by a separate pymongoimport subprocess. 
    '''
    
    parser = argparse.ArgumentParser( usage=usage_message, version=__VERSION__ )
    parser = add_standard_args( parser )
    parser.add_argument( "--autosplit", type=int, default=1,
                         help="split file based on loooking at the first ten lines and overall file size [default : %(default)s]")
    parser.add_argument( "--splitsize", type=int, default=10000, help="Split file into chunks of this size [default : %(default)s]" )
 
    args= parser.parse_args( *argv )
    
    log = Logger( "multiimport" ).log()
    
    Logger.add_file_handler( "multiimport" )
    Logger.add_stream_handler( "multiimport" )
    
    child_args = sys.argv[1:]
    children = OrderedDict()
    
    print( args.filenames )
    if len( args.filenames ) == 0 :
        log.info( "no input file to split")
        sys.exit( 0 )
    
    if args.autosplit or args.split_size :    
        if len( args.filenames) > 1 :
                log.warn( "More than one input file specified ( '%s' ) only splitting the first file:'%s'", 
                       " ".join( args.filenames ), args.filenames[ 0 ] )
        if args.autosplit:
            child_args = strip_arg( child_args, "--autosplit", True)
        if args.splitsize:
            child_args = strip_arg( child_args, "--splitsize", True )
        

            splitter = File_Splitter( args.filenames[ 0 ], args.hasheader )
            process_count = 0
    
        for i in args.filenames: # get rid of old filenames
            child_args = strip_arg( child_args, i, False )
        
    if args.autosplit:
        log.info( "Autosplitting file: '%s' into (approx) %i chunks", args.filenames[ 0 ], args.autosplit )
        files = splitter.autosplit( args.autosplit )
    elif args.splitsize:
        log.info( "Splitting file: '%s' into %i line chunks", args.filenames[ 0 ], args.splitsize )
        files = splitter.split_file( args.splitsize )
    else:
        files = args.filenames
        
    start = time.time()

    try :
        for filename in files:
            #log.info( "Processing '%s'", filename )
            process_count = process_count + 1
            proc_name = filename
            proc = Process( target=mongo_import, name=proc_name, args=child_args + [ "--logname", filename, "--silent", filename ] )
            children[ proc_name ] = { "process" : proc }
            log.info( "starting sub process: %s", proc_name )
            children[ proc_name ][ "start" ] = time.time()
            proc.start()
            
        for i in children.keys():
            #log.info( "Waiting for process: '%s' to complete" , i )
            children[ i ][ "process" ].join()
            children[ i ][ "end" ] = time.time()
            log.info( "elapsed time for process %s : %f", i, children[ i ][ "end" ] - children[ i ][ "start"])
            
    except KeyboardInterrupt :
        for i in children.keys():
            log.info( "terminating process: '%s'", i )
            children[ i ][ "process" ].terminate()
            
    finish = time.time()
    
    log.info( "Total elapsed time:%f" % ( finish - start ))
    
    
if __name__ == '__main__':
    multi_import( sys.argv[1:])

