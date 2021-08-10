import unittest

from pymongoimport.filesplitter import LineCounter
from pymongoimport.liner import make_line_file

class MyTestCase(unittest.TestCase):

	def test_Line_Counter(self):
        	self._test_file(1, filename="1.txt")

if __name__ == '__main__':
	unittest.main()

