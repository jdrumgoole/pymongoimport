#!/usr/bin/env python3

"""
Created on 19 Feb 2016

====================
 Mongoimport
====================

@author: jdrumgoole
"""

import argparse
import os
import sys
from multiprocessing import Process

import pymongo

from pymongoimport.argparser import add_standard_args
from pymongoimport.audit import Audit
from pymongoimport.command import Drop_Command, Generate_Fieldfile_Command, Import_Command
from pymongoimport.logger import Logger


# from monglog import MongoHandler

# def mongo_import_one( log, client, args, filename):
#
# def mongo_import( log, client, args, filenames):
#
#     if args.database:
#         database_name= args.database
#     else:
#         database_name = "PYIM"
#
#     if args.collection:
#         collection_name = args.collection
#     else:
#         collection_name = "ported"
#
#     database = client[database_name]
#     collection = database[collection_name]
#
#
#
#
#
#     if args.batchsize < 1:
#         log.warn("Chunksize must be 1 or more. Chunksize : %i", args.batchsize)
#         sys.exit(1)
#     try:
#
#         cmd = Import
#
#         file_processor = FileProcessor(collection, args.delimiter, args.onerror, args.id, args.batchsize, args.limit )
#         file_processor.processFiles(filenames=args.filenames,
#                                     field_filename=args.fieldfile,
#                                     hasheader=args.hasheader,
#                                     restart=args.restart,
#                                     audit=audit, batchID=batchID)
#
#         if args.audit:
#             audit.end_batch(batchID)
#
#     except KeyboardInterrupt:
#         log.warn("exiting due to keyboard interrupt...")

class Sub_Process(object):

    def __init__(self, log, audit, batch_ID, args):

        self._audit = audit
        self._batch_ID = batch_ID
        self._log = log
        self._host = args.host
        self._write_concern = args.writeconcern
        self._fsync = args.fsync
        self._journal = args.journal
        self._audit = args.audit
        self._database_name = args.database
        self._collection_name = args.collection
        self._fieldfile = args.fieldfile
        self._hasheader = args.hasheader
        self._delimiter = args.delimiter
        self._onerror = args.onerror
        self._limit = args.limit
        self._limit = args.limit
        self._args = args

    def setup_log_handlers(self):
        if self._log:
            self._log = Logger(self._args.logname, self._args.loglevel).log()

            # Logger.add_file_handler(args.logname)

            if not self._args.silent:
                Logger.add_stream_handler(self._args.logname)

    def run(self, filename):
        if self._log:
            self._log.info("Started pymongoimport")
        else:
            self._log = Logger(self._args.logname, self._args.loglevel).log()

            # Logger.add_file_handler(args.logname)

            if not self._args.silent:
                Logger.add_stream_handler(self._args.logname)

        if self._write_concern == 0:  # pymongo won't allow other args with w=0 even if they are false
            client = pymongo.MongoClient(self._host, w=self._write_concern)
        else:
            client = pymongo.MongoClient(self._host, w=self._write_concern, fsync=self._fsync, j=self._journal)

        if not self._database_name:
            self._database_name = "PYIM"

        if not self._collection_name:
            self._collection_name = "ported"

        database = client[self._database_name]
        self._collection = database[self._collection_name]

        if self._log:
            self._log.info("Write concern : %i", self._write_concern)
            self._log.info("journal       : %i", self._journal)
            self._log.info("fsync         : %i", self._fsync)
            self._log.info("hasheader     : %s", self._hasheader)

        cmd = Import_Command(log=self._log,
                             collection=self._collection,
                             field_filename=self._fieldfile,
                             delimiter=self._delimiter,
                             hasheader=self._hasheader,
                             onerror=self._onerror,
                             limit=self._limit,
                             audit=self._audit,
                             id=self._batch_ID)

        cmd.run(filename)

        return 1

    def process_batch(self, pool_size, files):

        procs = []
        for f in files[:pool_size]:
            self._log.info("Processing:'%s'", f)
            proc = Process(target=self.run, args=(f,), name=f)
            proc.start()
            procs.append(proc)

        for p in procs:
            p.join()

        return files[pool_size:]


def pymongoimport_main(input_args=None):
    """
    Expect to recieve an array of args
    
    1.3 : Added lots of support for the NHS Public Data sets project. --addfilename and --addtimestamp.
    Also we now fail back to string when type conversions fail.
    
    >>> pymongoimport_main( [ 'test_set_small.txt' ] )
    database: test, collection: test
    files ['test_set_small.txt']
    Processing : test_set_small.txt
    Completed processing : test_set_small.txt, (100 records)
    Processed test_set_small.txt
    """

    usage_message = """
    
    pymongoimport is a python program that will import data into a mongodb
    database (default 'test' ) and a mongodb collection (default 'test' ).
    
    Each file in the input list must correspond to a fieldfile format that is
    common across all the files. The fieldfile is specified by the 
    --fieldfile parameter.
    
    An example run:
    
    python pymongoimport.py --database demo --collection demo --fieldfile test_set_small.ff test_set_small.txt
    """

    # if input_args:
    #     print("args: {}".format( " ".join(input_args)))

    parser = argparse.ArgumentParser(usage=usage_message)
    parser = add_standard_args(parser)
    # print( "Argv: %s" % argv )
    # print(argv)

    if input_args:
        cmd = input_args
        args = parser.parse_args(cmd)
    else:
        cmd = tuple(sys.argv[1:])
        args = parser.parse_args(cmd)
        cmd_args = " ".join(cmd)
    # print("args: %s" % args)

    log = Logger(args.logname, args.loglevel).log()

    # Logger.add_file_handler(args.logname)

    if not args.silent:
        Logger.add_stream_handler(args.logname)

    log.info("Started pymongoimport")
    print(args.filenames)
    if args.genfieldfile:
        args.hasheader = True
        log.info("Forcing hasheader true for --genfieldfile")
        cmd = Generate_Fieldfile_Command(log, args.delimiter)
        for i in args.filenames:
            cmd.run(i)
        sys.exit(0)

    if args.writeconcern == 0:  # pymongo won't allow other args with w=0 even if they are false
        client = pymongo.MongoClient(args.host, w=args.writeconcern)
    else:
        client = pymongo.MongoClient(args.host, w=args.writeconcern, fsync=args.fsync, j=args.journal)

    if args.audit:
        audit = Audit(client=client)
        batch_ID = audit.start_batch({"command": input_args})
    else:
        audit = None
        batch_ID = None

    if args.database:
        database_name = args.database
    else:
        database_name = "PYIM"

    if args.collection:
        collection_name = args.collection
    else:
        collection_name = "ported"

    database = client[database_name]
    collection = database[collection_name]

    if args.drop:
        if args.restart:
            log.info("Warning --restart overrides --drop ignoring drop commmand")
        else:
            cmd = Drop_Command(log=log, audit=audit, id=batch_ID, database=database)
            cmd.run(collection_name)

    if args.filenames:

        if args.audit:
            audit = Audit(client=client)
            batch_ID = audit.start_batch({"command": sys.argv})
        else:
            audit = None
            batch_ID = None

        process = Sub_Process(log, audit, batch_ID, args)

        for i in args.filenames:
            if os.path.isfile(i):
                process.run(i)
            else:
                log.warning("No such file:'{}' ignoring".format(i))

        if args.audit:
            audit.end_batch(batch_ID)

    else:
        log.info("No input files: Nothing to do")

    return 1


if __name__ == '__main__':
    pymongoimport_main()
