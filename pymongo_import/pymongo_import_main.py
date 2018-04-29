#!/usr/bin/env python3

"""
Created on 19 Feb 2016

====================
 Mongoimport
====================

@author: jdrumgoole
"""


import argparse
import sys

import pymongo
from pymongo_import.fileprocessor import FileProcessor
from pymongo_import.fieldconfig import FieldConfig
from pymongo_import.argparser import add_standard_args
from pymongo_import.logger import Logger
from pymongo_import.audit import Audit

def mongo_import(input_args=None):
    """
    Expect to recieve an array of args
    
    1.3 : Added lots of support for the NHS Public Data sets project. --addfilename and --addtimestamp.
    Also we now fail back to string when type conversions fail.
    
    >>> mongo_import( [ 'test_set_small.txt' ] )
    database: test, collection: test
    files ['test_set_small.txt']
    Processing : test_set_small.txt
    Completed processing : test_set_small.txt, (100 records)
    Processed test_set_small.txt
    """

    usage_message = '''
    
    pymongo_import is a python program that will import data into a mongodb
    database (default 'test' ) and a mongodb collection (default 'test' ).
    
    Each file in the input list must correspond to a fieldfile format that is
    common across all the files. The fieldfile is specified by the 
    --fieldfile parameter.
    
    An example run:
    
    python pymongo_import.py --database demo --collection demo --fieldfile test_set_small.ff test_set_small.txt
    '''

    if input_args:
        print("args: {}".format( " ".join(input_args)))

    parser = argparse.ArgumentParser(usage=usage_message)
    parser = add_standard_args(parser)
    # print( "Argv: %s" % argv )
    # print(argv)

    if input_args :
        cmd = input_args
        args = parser.parse_args(cmd)
    else:
        cmd = tuple(sys.argv[1:])
        args = parser.parse_args(cmd)
    # print("args: %s" % args)

    log = Logger(args.logname, args.loglevel).log()

    Logger.add_file_handler(args.logname)

    if not args.silent:
        Logger.add_stream_handler(args.logname)

    log.info("Started pymongo_import")
    log.info("Write concern : %i", args.writeconcern)
    log.info("journal       : %i", args.journal)
    log.info("fsync         : %i", args.fsync)
    log.info("genfieldfile  : %s", args.genfieldfile)
    if args.genfieldfile:
        args.hasheader = True
        log.info("Forcing hasheader true for --genfieldfile")
    log.info("hasheader     : %s", args.hasheader)

    if args.writeconcern == 0:  # pymongo won't allow other args with w=0 even if they are false
        client = pymongo.MongoClient(args.host, w=args.writeconcern)
    else:
        client = pymongo.MongoClient(args.host, w=args.writeconcern, fsync=args.fsync, j=args.journal)

    database = client[args.database]
    collection = database[args.collection]

    if args.drop:
        if args.restart:
            log.info("Warning --restart overrides --drop ignoring drop commmand")
        else:
            database.drop_collection(args.collection)
            log.info("dropped collection: %s.%s", args.database, args.collection)

    if args.genfieldfile:
        for i in args.filenames:
            fc_filename = FieldConfig.generate_field_file(i, args.delimiter)
            log.info("Creating '%s' from '%s'", fc_filename, i)
        sys.exit(0)
    elif args.filenames:
        log.info("Using database: %s, collection: %s", args.database, args.collection)
        # log.info( "processing %i files", len( args.filenames ))

        if args.batchsize < 1:
            log.warn("Chunksize must be 1 or more. Chunksize : %i", args.batchsize)
            sys.exit(1)
        try:
            if args.audit:
                log.info( "Auditing output")
                audit = Audit(client)
                batchID = audit.start_batch({"cmd"  : cmd,
                                             "info" : args.info })
            else:
                audit=None
                batchID=None

            file_processor = FileProcessor(collection, args.delimiter, args.onerror, args.id, args.batchsize)
            file_processor.processFiles(filenames=args.filenames,
                                        field_filename=args.fieldfile,
                                        hasheader=args.hasheader,
                                        restart=args.restart,
                                        audit=audit, batchID=batchID)

            if args.audit:
                audit.end_batch(batchID)

        except KeyboardInterrupt:
            log.warn("exiting due to keyboard interrupt...")
    else:
        log.info("No input files: Nothing to do")

    return 1

if __name__ == '__main__':
    mongo_import()