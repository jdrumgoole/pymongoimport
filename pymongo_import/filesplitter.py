'''
Created on 13 Aug 2017

@author: jdrumgoole

=====================================
File_Splitter
=====================================

File Splitter is a class that takes a file and splits it into separate pieces. Its purpose built for
use wit pymongo_import and is expected to be used to split CSV files (which may or may not have
a header, hence the **hasheader** argument). When splitting a file the output files are produced without
a header file. 

The file can be split by number of lines using the **split_file** function. Alternatively 
the file may be split automatically into a number of pieces specified by as a parameter to
**autosplit**. Autosplitting is achieved by by guessing the average line size by looking at
the first ten lines and taking an average of those lines.

The output files have the same name as the input file with a number appended ( .1, .2, .3 etc.).

There is also a **count_lines** function to count the lines in a file.

'''
import os
from collections import OrderedDict
from enum import Enum


class File_Type(Enum):
    DOS = 1
    UNIX = 2

class File_Splitter(object):



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
        self._line_count = None
        self._header_line = ""  # Not none so len does something sensible when has_header is false
        self._size = os.path.getsize(self._input_filename)
        # self._data_lines_count = 0
        self._size_threshold = 1024 * 10
        self._split_size = None
        self._file_type = None
        self._autosplits = None
        self._splits = None

        self._check_file_type()


    def _check_file_type(self):
        line=None
        with open( self._input_filename, "r") as f:
            line = f.readline()
            if f.newlines and f.newlines == '\r\n':
                self._file_type = File_Type.DOS
            else:
                self._file_type = File_Type.UNIX

    def new_file(self, filename, ext):
        basename = os.path.basename(filename)
        filename = "%s.%i" % (basename, ext)
        # self._files[filename] = 0
        newfile = open(filename, "w")
        return (newfile, filename)

    def size(self, include_header=True, dos_adjust=False):
        """

        :param include_header: Include header size in size otherwise subtract
        :param dos_adjust: For DOS files deduct size increment due to extra LF characters
        :return: file size
        """

        file_size = None
        if include_header:
            file_size = self._size
        else:
            file_size = self._size - len( self._header_line)

        if dos_adjust:
            file_size = file_size - self.no_of_lines( include_header)

        return file_size

    @staticmethod
    def blocks(files, size=1024 * 64):
        while True:
            b = files.read(size)
            if not b: break
            yield b

    def count_lines(self):
        """
        Count all the lines in a file including the header if present.
        :param filename: The filename to count the lines for
        :return int : Number of lines in the file:
        """
        self._line_count=0
        block = None
        with open(self._input_filename, "r", encoding="utf-8", errors='ignore') as f:
            for block in File_Splitter.blocks(f):
                self._line_count = self._line_count + block.count("\n")

        if block and block[-1:] != '\n' : #file doesn't end with a newline but its still a line
            self._line_count = self._line_count + 1

        return self._line_count

    def no_of_lines(self, include_header=True):

        if self._line_count is None:
            self.count_lines()
        if include_header:
            return self._line_count
        else:
            return self._line_count - 1

    def wc(self):
        return (self.no_of_lines(), self.size)

    def copy_file(self, rhs, ignore_header=True):
        """
        Copy the input file to the file ;param rhs. If :param
        ignore_header is true the strip the header during copying.
        :param rhs:
        :param has_header:
        :return:
        """

        lhs = self._input_filename

        total_lines = 0
        with open(lhs, "r", encoding="utf-8", errors='ignore') as input:
            if ignore_header:
                self._header_line=input.readline()

            with open(rhs, "w", encoding="utf-8", errors='ignore') as output:
                for i in File_Splitter.blocks(input):
                    total_lines = total_lines + i.count("\n")
                    output.write(i)

            if i and i[-1:] != '\n' : #file doesn't end with a newline but its still a line
                total_lines = total_lines + 1

            if repr( input.newlines ) == "\r\n":
                self._file_type = File_Type.DOS
            else:
                self._file_type = File_Type.UNIX

        return (rhs, total_lines)

    def has_header(self):
        return self._header_line != ""

    def header_line(self):
        return self._header_line

    def no_header_size(self):
        return self._size - len(self._header_line)

    def output_files(self):
        return list(self._files.keys())

    # def data_lines_count(self):
    #     return self._data_lines_count


    def split_file(self, split_size=0):
        """
        Split file in a number of discrete parts of size split_size
        The last split may be less than split_size in size.
        This is a generator function that yields each split as it is
        created.

        :param split_size:
        :return: a generator of tuples (filename, split_size)
        Where split_size is the size of the split in bytes.
        """

        if split_size < 1 :
            yield self.copy_file(self._input_filename + ".1")
        else:
            with open(self._input_filename, "r") as input_file:

                current_split_size = 0
                file_count = 0
                filename = None
                output_file = None

                if self._has_header: #we strip the header from output files
                    self._header_line = input_file.readline()

                line = input_file.readline()
                #print( "Line type:%s" % repr(input_file.newlines))
                while line != "":
                    if current_split_size < split_size:
                        if current_split_size == 0:
                            file_count = file_count + 1
                            (output_file, filename) = self.new_file(self._input_filename, file_count)
                            #print( "init open:%s" % filename)
                    else:
                        assert current_split_size == split_size
                        output_file.close()
                        #print( "std close:%s" % filename)
                        yield (filename, current_split_size)
                        current_split_size = 0
                        file_count = file_count + 1
                        (output_file, filename) = self.new_file(self._input_filename, file_count)
                        #print("std open:%s" % filename)
                    output_file.write(line)
                    current_split_size = current_split_size + 1
                    line = input_file.readline()

                if repr(input_file.newlines) == "\r\n":
                    self._file_type = File_Type.DOS
                else:
                    self._file_type = File_Type.UNIX

            if current_split_size > 0: # if its zero we just closed the file and did a yield
                output_file.close()
                #print("final close:%s" % filename)
                yield (filename, current_split_size)

            #print("Exited: current_split_size: %i split_size: %i" % (current_split_size, split_size))

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
            if split_count > 0 :
                file_size = self._size

                total_lines = int(round(file_size / average_line_size))
                # print( "total lines : %i"  % total_lines )

                self._split_size = int(round(total_lines / split_count))
            else:
                self._split_size = 0

            #print("Splitting '%s' into at least %i pieces of size %i" % (
            #self._input_filename, split_count + 1, self._split_size))
            for i in self.split_file(self._split_size):
                yield i
