#!/usr/bin/env python
'''
Created on 19 Feb 2016

Need tests for skip larger than file size.

@author: jdrumgoole
'''
import argparse
import sys

from mongodb_utils.mongodb import MongoDB
from pymongodbimport.fileprocessor import FileProcessor 

def mainline( args ):
    
    __VERSION__ = "1.3"
    
    '''
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
    parser = argparse.ArgumentParser( prog = "pymongodbimport", usage=usage_message )
    parser.add_argument( '--database', default="test", help='specify the database name')
    parser.add_argument( '--collection', default="test", help='specify the collection name')
    parser.add_argument( '--host', default="mongodb://localhost:27017/test", help='mongodb://localhost:270017 : std URI arguments apply')
    parser.add_argument( '--chunksize', type=int, default=500, help='set chunk size for bulk inserts' )
    parser.add_argument( '--restart', default=False, action="store_true", help="use record count insert to restart at last write")
    parser.add_argument( '--drop', default=False, action="store_true", help="drop collection before loading" )
    parser.add_argument( '--ordered', default=False, action="store_true", help="forced ordered inserts" )
    parser.add_argument( '--verbose', default=0, type=int, help="controls how noisy the app is" )
    parser.add_argument( "--fieldfile", default= None, type=str,  help="Field and type mappings")
    parser.add_argument( "--delimiter", default=",", type=str, help="The delimiter string used to split fields (default ',')")
    parser.add_argument( 'filenames', nargs="*", help='list of files')
    parser.add_argument('--version', action='version', version='%(prog)s version:' + __VERSION__ )
    parser.add_argument('--addfilename', default=False, action="store_true", help="Add file name field to every entry" )
    parser.add_argument('--addtimestamp', default="none", choices=[ "none", "now", "gen" ], help="Add a timestamp to each record" )
    parser.add_argument('--hasheader',  default=False, action="store_true", help="Use header line for column names")
    parser.add_argument( '--genfieldfile', default=False, action="store_true", help="Generate a fieldfile from the data file")
    parser.add_argument( '--id', default="mongodb", choices=[ "mongodb", "gen"], help="Autogenerate ID default [ mongodb ]")
    
    args= parser.parse_args( args )
        
    client = MongoDB( args.host).client()
    database = client[ args.database ]
    collection = database[ args.collection ]
        
    if args.drop :
        if args.restart :
            print( "Warning --restart overrides --drop ignoring drop commmand")
        else:
            database.drop_collection( args.collection )
            print( "dropped collection: %s.%s" % ( args.database, args.collection ))
         
    if args.filenames:   
        print(  "Using database: %s, collection: %s" % ( args.database, args.collection ))
        print( "processing %i files" % len( args.filenames ))
    
        if args.chunksize < 1 :
            print( "Chunksize must be 1 or more. Chunksize : %i" % args.chunksize )
            sys.exit( 1 )
        
        file_processor = FileProcessor( collection )
        file_processor.processFiles( args )
            
    
if __name__ == '__main__':
    
    sys.argv.pop( 0 ) # remove script file name 
    mainline( sys.argv )
