'''
Created on 13 Aug 2017

@author: jdrumgoole
'''
import unittest
import os

from pymongo_import.filesplitter import File_Splitter, File_Type


class Test(unittest.TestCase):

    def test_count_lines(self):
        self.assertEqual(3, File_Splitter("data/threelines.txt").no_of_lines())
        self.assertEqual(0, File_Splitter("data/emptyfile.txt").no_of_lines())
        self.assertEqual(4, File_Splitter("data/fourlines.txt").no_of_lines())
        self.assertEqual(5, File_Splitter("data/inventory.csv").no_of_lines())
        self.assertEqual(4, File_Splitter("data/inventory.csv").no_of_lines( include_header=False))

    def _split_helper(self, filename, split_size, has_header=False, dos_adjust=False):

        splitter = File_Splitter(filename, has_header)

        count = 0
        part_total_size = 0
        part_total_count = 0

        for (part_name, line_count) in splitter.split_file(split_size):
            splitter_part = File_Splitter( part_name)
            part_count = splitter_part.no_of_lines()
            self.assertEqual(part_count, line_count)
            part_total_count = part_total_count + part_count
            part_total_size = part_total_size + splitter_part.size()
            os.unlink(part_name)

        self.assertEqual(part_total_count, splitter.no_of_lines( include_header=not has_header))
        self.assertEqual(part_total_size, splitter.size( include_header=not has_header, dos_adjust=dos_adjust))

    def test_split_file(self):
        self._split_helper("data/fourlines.txt", 3)
        self._split_helper("data/AandE_Data_2011-04-10.csv", 47, has_header=True, dos_adjust=True)
        self._split_helper("data/10k.txt", 2300, has_header=False)


    def _auto_split_helper(self, filename, split_count, has_header=False, dos_adjust=False):

        splitter = File_Splitter(filename, has_header=has_header)
        count = 0
        part_total_size = 0
        part_total_count = 0
        total_line_count = splitter.no_of_lines( include_header=has_header)

        for (part_name, line_count) in splitter.autosplit(split_count):
            splitter_part = File_Splitter( part_name)
            part_count = splitter_part.no_of_lines()
            self.assertGreater(part_count, 0)
            self.assertEqual(part_count, line_count)
            part_total_count = part_total_count + part_count
            part_total_size = part_total_size + splitter_part.size()
            os.unlink( part_name )

        self.assertEqual(part_total_count, splitter.no_of_lines( include_header=not has_header))
        self.assertEqual(part_total_size, splitter.size( include_header=False, dos_adjust=dos_adjust))

    def test_copy_file(self):
        splitter = File_Splitter("data/AandE_Data_2011-04-10.csv", has_header=True)
        self.assertEqual( splitter.file_type(), File_Type.DOS )
        (_, total_lines)=splitter.copy_file("data/AandE_Data_2011-04-10.csv" + ".1", ignore_header=True)

        self.assertEqual(File_Splitter("data/AandE_Data_2011-04-10.csv.1").size(),
                         splitter.size( include_header=False, dos_adjust=True))

    def test_autosplit_file(self):
        self._auto_split_helper("data/fourlines.txt", 2, has_header=False)
        self._auto_split_helper("data/ninelines.txt", 3, has_header=True)
        self._auto_split_helper("data/inventory.csv", 2, has_header=True)
        self._auto_split_helper("data/AandE_Data_2011-04-10.csv", 3, has_header=True, dos_adjust=True)
        self._auto_split_helper("data/AandE_Data_2011-04-10.csv", 2, has_header=True, dos_adjust=True)
        self._auto_split_helper("data/AandE_Data_2011-04-10.csv", 1, has_header=True, dos_adjust=True)
        self._auto_split_helper("data/AandE_Data_2011-04-10.csv", 0, has_header=True, dos_adjust=True)
        self._auto_split_helper("data/10k.txt", 5, has_header=True)

    def test_get_average_line_size(self):
        self.assertEqual(10, File_Splitter( "data/tenlines.txt").get_average_line_size())


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.test_autosplit']
    unittest.main()
