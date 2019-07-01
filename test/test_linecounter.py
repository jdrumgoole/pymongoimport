import unittest
from pymongoimport.liner import make_line_file
import os

from pymongoimport.filesplitter import LineCounter



class MyTestCase(unittest.TestCase):

    def _test_file(self, count, doseol=False):
        f = make_line_file(count=count, doseol=doseol)
        self.assertEqual(count, LineCounter(f).line_count())
        os.unlink(f)

    def test_Line_Counter(self):

        LineCounter()
        self._test_file(1)
        self._test_file(2)
        self._test_file(512)
        self._test_file(65000)
        self._test_file(1, doseol=True)
        self._test_file(10, doseol=True)
        self._test_file(65000,doseol=True)

if __name__ == '__main__':
    unittest.main()
