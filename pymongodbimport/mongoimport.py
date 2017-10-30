#!/usr/bin/env python
'''
Created on 19 Feb 2016

====================
Mongoimport
====================

This is a stub.

@author: jdrumgoole
'''
import argparse
import sys

import pymongo
from pymongodbimport.fileprocessor import FileProcessor 
from pymongodbimport.fieldconfig import FieldConfig
from pymongodbimport.argparser import add_standard_args
from pymongodbimport.logger import Logger

def mainline_argsparsed( args ):
    '''
    Expects the output of parse_args.
    '''
    
    log = Logger( args.logname, args.loglevel ).log()
    Logger.add_file_handler( args.logname  )
    
    if not args.silent:
        Logger.add_stream_handler( args.logname )
    
    log.info( "Started pymongodbimport")
    client = pymongo.MongoClient( args.host)
    database = client[ args.database ]
    collection = database[ args.collection ]
                
    if args.genfieldfile :
        args.hasheader = True
        
    if args.drop :
        if args.restart :
            log.info( "Warning --restart overrides --drop ignoring drop commmand")
        else:
            database.drop_collection( args.collection )
            log.info( "dropped collection: %s.%s", args.database, args.collection )
         
    if args.genfieldfile :
        for i in args.filenames :
            fc_filename = FieldConfig.generate_field_file( i, args.delimiter )
            log.info( "Creating '%s' from '%s'", fc_filename, i )
        sys.exit( 0 )
    elif args.filenames:   
        log.info(  "Using database: %s, collection: %s", args.database, args.collection )
        #log.info( "processing %i files", len( args.filenames ))
    
        if args.batchsize < 1 :
            log.warn( "Chunksize must be 1 or more. Chunksize : %i", args.batchsize )
            sys.exit( 1 )
        try :
            file_processor = FileProcessor( collection, args.delimiter, args.onerror, args.id, args.batchsize )
            file_processor.processFiles( args.filenames, args.hasheader, args.fieldfile, args.restart )
        except KeyboardInterrupt :
            log.warn( "exiting due to keyboard interrupt...")
    else:
        log.info( "No input files: Nothing to do") 
        
    return 1

def mongo_import( *argv ):
    
    __VERSION__ = "1.4.1"
    
    '''
    Expect to recieve sys.argv or similar
    
    1.3 : Added lots of support for the NHS Public Data sets project. --addfilename and --addtimestamp.
    Also we now fail back to string when type conversions fail.
    
    >>> mainline( [ 'test_set_small.txt' ] )
    database: test, collection: test
    files ['test_set_small.txt']
    Processing : test_set_small.txt
    Completed processing : test_set_small.txt, (100 records)
    Processed test_set_small.txt
    '''
    
    usage_message = '''
    
    pymongodbimport is a python program that will import data into a mongodb
    database (default 'test' ) and a mongodb collection (default 'test' ).
    
    Each file in the input list must correspond to a fieldfile format that is
    common across all the files. The fieldfile is specified by the 
    --fieldfile parameter.
    
    An example run:
    
    python pymongodbimport.py --database demo --collection demo --fieldfile test_set_small.ff test_set_small.txt
    '''
    
    parser = argparse.ArgumentParser( usage=usage_message, version= __VERSION__)
    parser = add_standard_args( parser )
    args= parser.parse_args( *argv )    
    return mainline_argsparsed( args )
    
if __name__ == '__main__':
    
    mongo_import( sys.argv[1:] )