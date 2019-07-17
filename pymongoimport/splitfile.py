"""
==================================================
splitfile : Split a file into seperate pieces
==================================================
Files can be split on
preset line boundaries using **--splitsize** or split automatically
into a preset number of pieces using **--autosplit**.

We include the **--hasheader** for use with csv files as we don't want
to include the header line in any of the input files.

**--autosplit** *<number of splits*
    Split a file into several chunks by looking at the first ten lines
    and using that to work out the average line os_size. We then use that os_size
    to determine how many lines each chunk needs to have to return *<number of splits>*
    splits.

**--hasheader**
    lets the program know this file has a header line. We do not include
    header lines in any of the split file outputs.

**--splitsize** *<no of lines>*
    Split a file into a specific number of chunks of os_size *<no of lines>*.

**filename**
    Name of file to split

Created on 11 Aug 2017

@author: jdrumgoole

"""
import argparse
import os
import sys

from pymongoimport.filesplitter import File_Splitter
from pymongoimport.version import __VERSION__


def split_file_main(*argv):
    usage_message = '''
    
Split a text file into seperate pieces. if you specify 
autosplit then the program will use the first ten lines 
to calcuate an average line os_size and use that to 
determine the rough number of splits.

if you use **--splitsize** then the file will be split 
using **--splitsize** chunks until it is consumed.
'''

    parser = argparse.ArgumentParser(usage=usage_message)

    parser.add_argument('-v", ''--version', action='version', version='%(prog)s ' + __VERSION__)
    parser.add_argument("--autosplit", type=int,
                        help="split file based on loooking at the first ten lines and overall file os_size [default : %(default)s]")
    parser.add_argument('--hasheader', default=False, action="store_true",
                        help="Ignore header when calculating splits, don't include header in output")
    parser.add_argument('--usefieldfile', type=str,
                        help="Use this field file and copy to match split filenames")
    parser.add_argument('--generatefieldfile', default=False, action="store_true",
                        help="Generate a fieldfile for each input file")
    parser.add_argument('--delimiter', default=",", help="Delimiter for fields[default : %(default)s] ")
    parser.add_argument("--splitsize", type=int, help="Split file into chunks of this os_size")
    parser.add_argument('--verbose', default=False, action="store_true",
                        help="Print out what is happening")
    parser.add_argument("filenames", nargs="*", help='list of files')
    args = parser.parse_args(*argv)

    if len(args.filenames) == 0:
        print("No input file specified to split")
        sys.exit(0)

    files = []

    for source in args.filenames:

        if not os.path.isfile(source):
            print(f"No such input file:'{source}'")
            continue

        splitter = File_Splitter(source, args.hasheader)

        if args.autosplit:
            if args.verbose:
                print(f"Autosplitting: '{source}' into approximately {args.autosplit} parts")
            for name, size in splitter.autosplit(args.autosplit):
                files.append((name, size))
        else:
            if args.verbose:
                print("Splitting '%s' using %i splitsize" % (args.filenames[0], args.splitsize))
            for name, size in splitter.splitfile(args.splitsize):
                files.append((name, size))

        # print( "Split '%s' into %i parts"  % ( args.filenames[ 0 ], len( files )))

        count = 1
        total_size = 0
        total_lines = 0
        results = list(files)
        for name, lines in results:
            size = os.path.getsize(name)
            total_size = total_size + size
            total_lines = total_lines + lines
            if args.verbose:
                print(f"{count:4}. '{name:20}'. Lines : {lines:6}, Size: {size:10}")

            count = count + 1
        if len(files) > 1:
            if args.verbose:
                print(f"{source} {total_lines:16} {total_size:17}")

        # if len(files) > 1:
        #     if args.verbose:
        #         print("{} {:16} {:17}".format(" " * (len(i) + 7), total_lines, total_size))

        if files and (total_size != splitter.no_header_size()):
            raise ValueError(f"Filesize of '{source}' and total size of pieces {files} #"
                             f"do not match: total_size: {total_size}, #"
                             f"no header split_size: {splitter.no_header_size()}")



    return results


if __name__ == '__main__':
    split_file_main(sys.argv[1:])
