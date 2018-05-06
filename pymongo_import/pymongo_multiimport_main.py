"""
@author: jdrumgoole
"""
import argparse
import sys
import multiprocessing
from multiprocessing import Pool, Process
from collections import OrderedDict
import time
import pymongo
import logging

from pymongo_import.filesplitter import File_Splitter

from pymongo_import.logger import Logger
from pymongo_import.argparser import add_standard_args
from pymongo_import.pymongo_import_main import Sub_Process

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

def chunker(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))

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
    parser.add_argument("--poolsize", type=int, default=multiprocessing.cpu_count(), help="The number of parallel processes to run")
    parser.add_argument("--forkmethod", choices=["spawn", "fork", "forkserver"], default="spawn",
                        help="The model used to define how we create subprocesses (default:'spawn')")

    args = parser.parse_args(*argv)

    multiprocessing.set_start_method(args.forkmethod)

    log = Logger("multi_import").log()

    Logger.add_file_handler("multi_import")
    Logger.add_stream_handler("multi_import")

    child_args = sys.argv[1:]
    children = OrderedDict()

    log.info("filenames:%s", args.filenames)
    if len(args.filenames) == 0:
        log.info("no input files")
        sys.exit(0)

    if args.poolsize:
        poolsize = args.poolsize
        child_args = strip_arg(child_args, "--poolsize", True)

    if args.restart:
        log.info("Ignoring --drop overridden by --restart")
    elif args.drop:
        client = pymongo.MongoClient(args.host)
        log.info("Dropping database : %s", args.database)
        client.drop_database(args.database)
        child_args = strip_arg(child_args, args.drop)


    start = time.time()

    process_count = 0
    log.info( "Poolsize:{}".format(poolsize))

    if args.forkmethod == "fork":
        subprocess = Sub_Process(log=log, audit=None, batch_ID=None, args=args)
    elif args.forkmethod == "spawn":
        subprocess = Sub_Process(log=None, audit=None, batch_ID=None, args=args)
    elif args.forkmethod == "forkserver":
        subprocess = Sub_Process(log=None, audit=None, batch_ID=None, args=args)

    subprocess.setup_log_handlers()

    # with Pool(poolsize) as p :
    #
    #     for i in args.filenames:
    #         print("Pool")
    #         p.apply_async(func=subprocess.run, args=(i,), callback=success_callback,
    #                       error_callback=fail_callback )
    #
    #     #results = [p.apply_async(subprocess.run, (i,)) for i in args.filenames]


    try:

        #
        # Should use a Pool here but Pools need top level functions which is
        # ugly.
        #
        proc_list=[]


        for arg_list in chunker( args.filenames, poolsize):

            proc_list=[]
            for i in arg_list:
                    log.info("Processing:'%s'", i)
                    proc = Process(target=subprocess.run, args=(i,), name=i)
                    proc.start()
                    proc_list.append(proc)

            for proc in proc_list:
                proc.join()

    except KeyboardInterrupt:
        log.info( "Keyboard interrupt...")
        for i in proc_list:
            log.info("terminating process: '%s'", proc_list[i].name)
            proc_list[i].terminate()

    finish = time.time()

    log.info("Total elapsed time:%f" % (finish - start))

if __name__ == '__main__':

    multi_import(sys.argv[1:])

    # with Pool(poolsize) as p:
    #     try:
    #         log.info( "processing args:%s", args.filenames)

            #for i in args.filenames:
            #print( "processing: {}".format(i))
            #p.map(subprocess.run, ['test_result_2016.txt.1', 'test_result_2016.txt.2', 'test_result_2016.txt.3', 'test_result_2016.txt.4', 'test_result_2016.txt.5'])
            #p.map(print_name, list(args.filenames))
                #subprocess.run(i)
            # for filename in args.filenames:
            #
            #     process_count = process_count + 1
            #     # need to turn args to Process into a tuple )
            #     new_args = copy.deepcopy(child_args)
            #     # new_args.extend( [ "--logname", filename[0], filename[0] ] )
            #     new_args.append(filename)
            #     log.info("Processing '%s'", filename)
            #     log.info("args:%s", new_args)
            #     process_pool.apply_async(func=shim, args=(new_args,))


            #log.info("elapsed time for process %s : %f", i, children[i]["end"] - children[i]["start"])




