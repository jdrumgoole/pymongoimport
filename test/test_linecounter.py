import unittest
from test.liner import make_line_file
import os

from pymongo_import.filesplitter import Line_Counter

def test_file( count, doseol=False):
    f = make_line_file(count=count, doseol=doseol)
    x = Line_Counter(f)
    os.unlink(f)

class MyTestCase(unittest.TestCase):

    def test_Line_Counter(self):

        Line_Counter()
        test_file(1)
        test_file(2)
        test_file(512)
        test_file(65000)
        test_file(1, doseol=True)
        test_file(10, doseol=True)
        test_file(65000,doseol=True)

if __name__ == '__main__':
    unittest.main()
