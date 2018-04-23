"""
@author: jdrumgoole
"""
import argparse
import sys
from multiprocessing import Pool
from collections import OrderedDict
import time
import copy
import os
import pymongo
import glob

from pymongo_import.filesplitter import File_Splitter
from pymongo_import.pymongo_import_main import mongo_import
from pymongo_import.logger import Logger
from pymongo_import.argparser import add_standard_args


def strip_arg(arg_list, remove_arg, has_trailing=False):
    '''
    Remove arg and arg argument from a list of args. If has_trailing is true then
    remove --arg value else just remove --arg.
    
    Args:
    
    arg_list (list) : List of args that we want to remove items from
    remove_arg (str) : Name of arg to remove. Must match element in `arg_list`.
    has_trailing (boolean) : If the arg in `remove_arg` has an arg. Then make sure
    to remove that arg as well
    '''
    try:
        location = arg_list.index(remove_arg)
        if has_trailing:
            del arg_list[location + 1]
        del arg_list[location]

    except ValueError:
        pass

    return arg_list


def multi_import(*argv):
    """
.. function:: multi_import ( *argv )

   Import CSV files using multiprocessing

   :param argv: list of command lines

   """

    usage_message = '''
    
    A master script to manage uploading of a single data file as multiple input files. Multi-import
    will optionally split a single file (specified by the --single argument) or optionally upload an
    already split list of files passed in on the command line.
    Each file is uplaoded by a separate pymongoimport subprocess. 
    '''

    parser = argparse.ArgumentParser(usage=usage_message)
    parser = add_standard_args(parser)
    parser.add_argument("--autosplit", type=int,
                        help="split file based on loooking at the first ten lines and overall file size [default : %(default)s]")
    parser.add_argument("--splitsize", type=int, default=0, help="Split file into chunks of this size [default : %(default)s]")
    parser.add_argument("--usesplits", action="store_true", default=False, help="Use the split files already created by by a previous autosplit")
    parser.add_argument("--poolsize", type=int, default=2, help="The number of parallel processes to run")
    args = parser.parse_args(*argv)

    log = Logger("multi_import").log()

    Logger.add_file_handler("multi_import")
    Logger.add_stream_handler("mult_iimport")

    child_args = sys.argv[1:]
    children = OrderedDict()

    print(args.filenames)
    if len(args.filenames) == 0:
        log.info("no input file to split")
        sys.exit(0)

    if args.autosplit or args.splitsize or args.usesplits or args.poolsize:
        if len(args.filenames) > 1:
            log.warn("More than one input file specified ( '%s' ) only splitting the first file:'%s'",
                     " ".join(args.filenames), args.filenames[0])
        if args.autosplit:
            child_args = strip_arg(child_args, "--autosplit", True)
        if args.splitsize:
            child_args = strip_arg(child_args, "--splitsize", True)
        if args.usesplits:
            child_args = strip_arg(child_args, "--usesplits", False)
        if args.poolsize:
            child_args = strip_arg(child_args, "--poolsize", True)

        splitter = File_Splitter(args.filenames[0], args.hasheader)

    for i in args.filenames:  # get rid of old filenames
        child_args = strip_arg(child_args, i, False)

    if args.usesplits:
        files = glob.glob( "*.[123456789]*" )
        files.sort()
        files = [ ( i, os.path.getsize(i)) for i in files]
        #print("usesplits:%s" % files)
    elif args.autosplit:
        log.info("Autosplitting file: '%s' into (approx) %i chunks", args.filenames[0], args.autosplit)
        files = splitter.autosplit(args.autosplit)
    elif args.splitsize > 0:
        log.info("Splitting file: '%s' into %i line chunks", args.filenames[0], args.splitsize)
        files = splitter.split_file(args.splitsize)
    else:
        files = [ ( i, os.path.getsize(i)) for i in args.filenames]

    if args.restart:
        log.info("Ignoring --drop overridden by --restart")
    elif args.drop:
        client = pymongo.MongoClient(args.host)
        log.info("Dropping database : %s", args.database)
        client.drop_database(args.database)
        child_args = strip_arg(child_args, args.drop)

    start = time.time()

    process_count = 0
    process_pool = Pool(processes=args.poolsize)
    try:
        for filename in files:
            process_count = process_count + 1
            proc_name = filename[0]
            # need to turn args to Process into a tuple )
            new_args = copy.deepcopy(child_args)
            # new_args.extend( [ "--logname", filename[0], filename[0] ] )
            new_args.extend([filename[0]])
            process_pool.apply_async(func=mongo_import, args=(new_args,))
            log.info("Processing '%s'", filename[0])

        process_pool.close()
        process_pool.join()
        #log.info("elapsed time for process %s : %f", i, children[i]["end"] - children[i]["start"])

    except KeyboardInterrupt:
        for i in children.keys():
            log.info("terminating pool: '%s'", i)
            process_pool.terminate()

    finish = time.time()

    log.info("Total elapsed time:%f" % (finish - start))


if __name__ == '__main__':
    multi_import(sys.argv[1:])
