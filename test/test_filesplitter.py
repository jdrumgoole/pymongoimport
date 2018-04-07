'''
Created on 13 Aug 2017

@author: jdrumgoole
'''
import unittest
import os

from pymongodbimport.filesplitter import File_Splitter


class Test(unittest.TestCase):

    def test_count_lines(self):
        self.assertEqual(3, File_Splitter.count_lines("test/threelines.txt"))
        self.assertEqual(0, File_Splitter.count_lines("test/emptyfile.txt"))
        self.assertEqual(4, File_Splitter.count_lines("test/fourlines.txt"))
        self.assertEqual(5, File_Splitter.count_lines("test/inventory.csv"))

    def _split_helper(self, filename, split_size, has_header=False):

        splitter = File_Splitter(filename, has_header)

        count = 0
        part_total_size = 0
        part_total_count = 0
        total_line_count = File_Splitter.count_lines(filename)
        if has_header:
            total_line_count = total_line_count - 1

        for (part_name, line_count) in splitter.split_file(split_size):
            part_count = File_Splitter.count_lines(part_name)
            self.assertEqual(part_count, line_count)
            part_total_count = part_total_count + part_count
            part_total_size = part_total_size + os.path.getsize(part_name)
            os.unlink(part_name)

        self.assertEqual(part_total_count, total_line_count)
        self.assertEqual(part_total_size, os.path.getsize(filename) - len(splitter.header_line()))

    def test_split_file(self):
        self._split_helper("test/fourlines.txt", 3)
        self._split_helper("test/AandE_Data_2011-04-10.csv", 47, has_header=True)
        self._split_helper("test/10k.txt", 2300, has_header=False)

    def _auto_split_helper(self, filename, split_count, has_header=False):

        splitter = File_Splitter(filename, has_header=has_header)
        count = 0
        part_total_size = 0
        part_total_count = 0
        total_line_count = File_Splitter.count_lines(filename)
        if has_header:
            total_line_count = total_line_count - 1

        for (part_name, line_count) in splitter.autosplit(split_count):
            part_count = File_Splitter.count_lines(part_name)
            self.assertGreater(part_count, 0)
            self.assertEqual(part_count, line_count)
            part_total_count = part_total_count + part_count
            part_total_size = part_total_size + os.path.getsize(part_name)
            # os.unlink( part_name )

        self.assertEqual(part_total_count, total_line_count)
        self.assertEqual(part_total_size, os.path.getsize(filename) - len(splitter.header_line()), filename)

    def test_copy_file(self):
        splitter = File_Splitter("test/AandE_Data_2011-04-10.csv", has_header=True)
        splitter.copy_file("test/AandE_Data_2011-04-10.csv" + ".1")
        self.assertGreater(len(splitter.header_line()), 0)
        self.assertEqual(os.path.getsize("test/AandE_Data_2011-04-10.csv.1"),
                         os.path.getsize("test/AandE_Data_2011-04-10.csv") - len(splitter.header_line()))

    def test_autosplit_file(self):
        self._auto_split_helper("test/fourlines.txt", 2)
        self._auto_split_helper("test/AandE_Data_2011-04-10.csv", 3, has_header=True)
        self._auto_split_helper("test/AandE_Data_2011-04-10.csv", 2, has_header=True)
        self._auto_split_helper("test/AandE_Data_2011-04-10.csv", 1, has_header=True)
        self._auto_split_helper("test/AandE_Data_2011-04-10.csv", 0, has_header=True)
        self._auto_split_helper("test/10k.txt", 5, has_header=True)

    def test_get_average_line_size(self):
        self.assertEqual(10, File_Splitter.get_average_line_size("test/tenlines.txt"))


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.test_autosplit']
    unittest.main()
