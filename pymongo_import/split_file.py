'''
==================================================
split_file : Split a file into seperate pieces
==================================================
Files can be split on
preset line boundaries using **--splitsize** or split automatically
into a preset number of pieces using **--autosplit**.

We include the **--hasheader** for use with csv files as we don't want
to include the header line in any of the input files.

**--autosplit** *<number of splits*
    Split a file into several chunks by looking at the first ten lines
    and using that to work out the average line size. We then use that size
    to determine how many lines each chunk needs to have to return *<number of splits>*
    splits.

**--hasheader**
    lets the program know this file has a header line. We do not include
    header lines in any of the split file outputs.

**--splitsize** *<no of lines>*
    Split a file into a specific number of chunks of size *<no of lines>*.

**filename**
    Name of file to split

Created on 11 Aug 2017

@author: jdrumgoole

'''
import argparse
import sys
import os
import shutil
from pymongo_import.version import __VERSION__

from pymongo_import.filesplitter import File_Splitter
from pymongo_import.fieldconfig import FieldConfig


def split_file(*argv):
    usage_message = '''
    
Split a text file into seperate pieces. if you specify 
autosplit then the program will use the first ten lines 
to calcuate an average line size and use that to 
determine the rough number of splits.

if you use **--splitsize** then the file will be split 
using **--splitsize** chunks until it is consumed.
'''

    parser = argparse.ArgumentParser(usage=usage_message)

    parser.add_argument('-v", ''--version', action='version', version='%(prog)s ' + __VERSION__)
    parser.add_argument("--autosplit", type=int,
                        help="split file based on loooking at the first ten lines and overall file size [default : %(default)s]")
    parser.add_argument('--hasheader', default=False, action="store_true",
                        help="Ignore header when calculating splits")
    parser.add_argument('--usefieldfile', type=str,
                        help="Use this field file and copy to match split filenames")
    parser.add_argument('--generatefieldfile', default=False, action="store_true",
                        help="Generate a fieldfile for each input file")
    parser.add_argument('--delimiter', default=",", help="Delimiter for fields[default : %(default)s] ")
    parser.add_argument("--splitsize", type=int, help="Split file into chunks of this size")
    parser.add_argument("filenames", nargs="*", help='list of files')
    args = parser.parse_args(*argv)

    if len(args.filenames) == 0:
        print("No input file specified to split")
        sys.exit(0)

    files = []

    for i in args.filenames:

        if not os.path.isfile( i ):
            print( "No such input file:'{}'".format(i))
            continue
        master_cfg_filename = FieldConfig.generate_field_filename( i, ".ff")
        if args.generatefieldfile:
            field_file = FieldConfig.generate_field_file( i, delimiter=args.delimiter)
        if not os.path.isfile( master_cfg_filename):
            raise OSError( "Missing field config file '{}' for '{}'".format( master_cfg_filename, i))

        splitter = File_Splitter(i, args.hasheader)

        if args.autosplit:
            print("Autosplitting: '{}' into approximately {} parts".format(i, args.autosplit))
            for newfile in splitter.autosplit(args.autosplit):
                cfg_filename = FieldConfig.generate_field_filename( newfile[0], ".ff")
                shutil.copy( master_cfg_filename, cfg_filename)
                files.append(newfile)
        else:
            print("Splitting '%s' using %i splitsize" % (args.filenames[0], args.splitsize))
            for newfile in splitter.split_file(args.splitsize):
                cfg_filename = FieldConfig.generate_field_filename( newfile[0], ".ff")
                shutil.copy( master_cfg_filename, cfg_filename)
                files.append(newfile)

        # print( "Split '%s' into %i parts"  % ( args.filenames[ 0 ], len( files )))

    count = 1
    total_size = 0
    total_lines = 0
    results = list(files)
    for (i, lines) in results:
        size = os.path.getsize(i)
        total_size = total_size + size
        total_lines = total_lines + lines
        print("{:4}. '{:20}'. Lines : {:5}, Size: {:7}".format(count, i, lines, size))

        count = count + 1
    if len(files) > 1 :
        print("{} {:15} {:14}".format( " " * (len(i) + 7), total_lines, total_size))

    if files and (total_size != splitter.no_header_size()):
        raise ValueError("Filesize of original and pieces does not match: total_size: %i, no header split_size: %i" % (
        total_size, splitter.no_header_size()))

    return results


if __name__ == '__main__':
    split_file(sys.argv[1:])