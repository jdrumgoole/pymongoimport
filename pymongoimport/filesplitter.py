'''
Created on 13 Aug 2017

@author: jdrumgoole

=====================================
File_Splitter
=====================================

File Splitter is a class that takes a file and splits it into separate pieces. Its purpose built for
use with pymongoimport and is expected to be used to split CSV files (which may or may not have
a header, hence the **hasheader** argument). When splitting a file the output files are produced without
a header file. 

The file can be split by number of lines using the **splitfile** function. Alternatively
the file may be split automatically into a number of pieces specified by as a parameter to
**autosplit**. Autosplitting is achieved by by guessing the average line size by looking at
the first ten lines and taking an average of those lines.

The output files have the same name as the input file with a number appended ( .1, .2, .3 etc.).

There is also a **count_lines** function to count the lines in a file.

'''
import os
from collections import OrderedDict
from enum import Enum


class Block_Reader(object):
    BLOCK_SIZE = 64 * 1024

    def __init__(self, filename, blocksize=None):

        self._filename = filename

        if blocksize:
            self._blocksize = blocksize
        else:
            self._blocksize = Block_Reader.BLOCK_SIZE

    def __enter__(self):
        self._file = open(self.filename, "r", encoding="utf-8", errors='ignore', newline='')
        return self._file

    def __exit__(self, *args):
        self._file.close()

    @staticmethod
    def read_blocks(file, blocksize=None):

        if not blocksize:
            blocksize = Block_Reader.BLOCK_SIZE

        while True:
            # disable universal newlines so that sizes are correct when
            # reading DOS and Linux files.
            b = file.read(blocksize)
            if not b:
                break
            yield b

    @staticmethod
    def readline(file):
        return file.readline()

    def read_fd(self, fd):
        for block in self.read_blocks(fd, self._blocksize):
            yield block

    def read_file(self, filename):
        with open(filename, "r", encoding="utf-8", errors='ignore', newline='') as f:
            yield from self.read_fd(f)


class FileType(Enum):
    DOS = 1
    UNIX = 2


class LineCounter(object):
    """
    Count the lines in a file efficiently by reading in a block
    at a time and counting '\n' chars. Blocks are large by
    default (64k).
    """

    def __init__(self, filename=None, blocksize=None, count_now=True):

        self._first_line = None
        self._line_count = None
        self._file_size = 0
        if blocksize:
            self._blocksize = blocksize
        else:
            self._blocksize = 64 * 1024

        self._filename = filename

        if count_now and filename:
            self.count_now(self._filename)

    def line_count(self):
        return self._line_count

    def first_line(self):
        return self._first_line

    def file_size(self):
        return self._file_size

    def count_now(self, filename):
        self._file_size = 0
        self._line_count = 0
        block = None
        self._reader = Block_Reader(self._blocksize)
        # disable universal newlines with "newline=''" so that sizes are correct when
        # reading DOS and Linux files.
        for i in self._reader.read_file(filename):
            block = i
            self._line_count = self._line_count + i.count("\n")
            self._file_size = self._file_size + len(i)

        if block and block[-1:] != '\n':  # file doesn't end with a newline but its still a line
            self._line_count = self._line_count + 1

        # print( "getsize({}}, self._file_size: {}".format( os.path.getsize(filename), self._file_size))
        assert (os.path.getsize(filename) == self._file_size)
        return (self._file_size, self._line_count)

    @staticmethod
    def skipLines(f, skipCount):
        '''
        >>> f = open( "test_set_small.txt", "r" )
        >>> skipLines( f , 20 )
        20
        '''

        lineCount = 0
        if (skipCount > 0):
            # print( "Skipping")
            dummy = f.readline()  # skipCount may be bigger than the number of lines i  the file
            while dummy:
                lineCount = lineCount + 1
                if (lineCount == skipCount):
                    break
                dummy = f.readline()

        return lineCount


class File_Splitter(object):
    """
    Split a file into a number of segments. You can autosplit a file into a specific
    number of pieces (autosplit) or divide in segments of a specific size (splitfile)
    """

    def __init__(self, input_filename, has_header=False):
        """

        Need to work out how to get line_count etc. consist for unit testing. Needs to be
        canonical for DOS and UNIX files.

        WIP

        :param input_filename : The file to be split
        has_header : Does this file have a header line
        """
        self._input_filename = input_filename
        self._has_header = has_header
        lc = LineCounter(self._input_filename)
        self._line_count = lc.line_count()
        self._size = lc.file_size()
        self._header_line = ""  # Not none so len does something sensible when has_header is false
        self._size = os.path.getsize(self._input_filename)
        if self._size > 0 and self._has_header:
            self._header_line = self.get_header(self._input_filename)
        # self._data_lines_count = 0
        self._size_threshold = 1024 * 10
        self._split_size = None
        self._file_type = None
        self._autosplits = None
        self._splits = None

        self._check_file_type()

    def get_header(self, filename):
        with open(filename, "r") as f:
            return f.readline()

    def _check_file_type(self):
        line = ""
        with open(self._input_filename, "r") as f:
            line = f.readline()
            if f.newlines and f.newlines == '\r\n':
                self._file_type = FileType.DOS
            else:
                self._file_type = FileType.UNIX
        return line

    def new_file(self, filename, ext):
        basename = os.path.basename(filename)
        filename = "{}.{}".format(basename, ext)
        # self._files[filename] = 0
        newfile = open(filename, "w")
        return (newfile, filename)

    def size(self):
        return self._size

    def line_count(self):
        return self._line_count

    # def size(self, include_header=True, dos_adjust=False):
    #     """
    #
    #     :param include_header: Include header size in size otherwise subtract
    #     :param dos_adjust: For DOS files deduct size increment due to extra LF characters
    #     :return: file size
    #     """
    #
    #     file_size = None
    #     if include_header:
    #         file_size = self._size
    #     else:
    #         file_size = self._size - len( self._header_line)
    #
    #     if dos_adjust:
    #         file_size = file_size - self._line_count
    #
    #     return file_size

    def wc(self):
        return self._line_count, os.path.getsize(self._input_filename)

    def copy_file(self, rhs, ignore_header=True):
        """
        Copy the input file to the file ;param rhs. If :param
        ignore_header is true the strip the header during copying.
        :param rhs:
        :param has_header:
        :return:
        """

        lhs = self._input_filename

        lhs_reader = Block_Reader(lhs)
        total_lines = 0
        with open(lhs, "r", encoding="utf-8", errors='ignore') as input_file:
            if ignore_header:
                self._header_line = Block_Reader.readline(input_file)

            with open(rhs, "w", encoding="utf-8", errors='ignore') as output_file:
                for i in Block_Reader.read_blocks(input_file):
                    total_lines = total_lines + i.count("\n")
                    output_file.write(i)

            if i and i[-1:] != '\n':  # file doesn't end with a newline but its still a line
                total_lines = total_lines + 1

        return (rhs, total_lines)

    def has_header(self):
        return self._header_line != ""

    def header_line(self):
        return self._header_line

    def no_header_size(self):
        """
        For DOS files the line endings have an extra character.
        :return:
        """

        if self._has_header:
            if self._file_type == FileType.DOS:
                adjustment = self._line_count + len(self._header_line)  # tryout
            else:
                adjustment = len(self._header_line)
        else:
            adjustment = 0

        return self._size - adjustment

    def output_files(self):
        return list(self._files.keys())

    # def data_lines_count(self):
    #     return self._data_lines_count

    def splitfile(self, split_size=0):
        """
        Split file in a number of discrete parts of size split_size
        The last split may be less than split_size in size.
        This is a generator function that yields each split as it is
        created.

        :param split_size:
        :return: a generator of tuples (filename, split_size)
        Where split_size is the size of the split in bytes.
        """

        if split_size < 1:
            yield self.copy_file(self._input_filename + ".1")
        else:
            with open(self._input_filename, "r") as input_file:

                current_split_size = 0
                file_count = 0
                filename = None
                output_file = None

                if self._has_header:  # we strip the header from output files
                    self._header_line = input_file.readline()

                for line in input_file:
                # print( "Line type:%s" % repr(input_file.newlines))
                    if current_split_size < split_size:
                        if current_split_size == 0:
                            file_count = file_count + 1
                            (output_file, filename) = self.new_file(self._input_filename, file_count)
                            # print( "init open:%s" % filename)
                    else:
                        assert current_split_size == split_size
                        output_file.close()
                        # print( "std close:%s" % filename)
                        yield (filename, current_split_size)
                        current_split_size = 0
                        file_count = file_count + 1
                        (output_file, filename) = self.new_file(self._input_filename, file_count)
                        # print("std open:%s" % filename)
                    output_file.write(line)
                    current_split_size = current_split_size + 1

            if current_split_size > 0:  # if its zero we just closed the file and did a yield
                output_file.close()
                # print("final close:%s" % filename)
                yield (filename, current_split_size)

            # print("Exited: current_split_size: %i split_size: %i" % (current_split_size, split_size))

    def file_type(self):
        return self._file_type

    def get_average_line_size(self, sample_size=10):
        """
        Read the first sample_size lines of a file (ignoring the header). Use these lines to estimate the
        average line size.
        :return: average_line_size
        """

        line_sample = 10
        count = 0
        line = None

        with open(self._input_filename, "r") as f:
            if self._has_header:
                line = f.readline()
                self._header_line = line

            line = f.readline()
            while line and count < line_sample:
                count = count + 1
                line = f.readline()
                sample_size = sample_size + len(line)

        if count > 0:
            return int(round(sample_size / count))
        else:
            return 0

    @staticmethod
    def shim_names(g):
        for i in g:
            yield i[0]

    def split_size(self):
        return self._split_size

    def autosplit(self, split_count):

        average_line_size = self.get_average_line_size()

        if average_line_size > 0:
            if split_count > 0:
                file_size = self._size

                total_lines = int(round(file_size / average_line_size))
                # print( "total lines : %i"  % total_lines )

                self._split_size = int(round(total_lines / split_count))
            else:
                self._split_size = 0

            # print("Splitting '%s' into at least %i pieces of size %i" % (
            # self._input_filename, split_count + 1, self._split_size))
            for i in self.splitfile(self._split_size):
                yield i
