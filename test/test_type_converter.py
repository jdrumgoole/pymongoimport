import unittest

from pymongo_import.type_converter import Converter
import datetime

class Test(unittest.TestCase):

    def test_converter(self):

        c =Converter()

        self.assertEqual( 10, c.convert( "int", "10"))
        self.assertEqual( 10.0, c.convert( "int", "10.0"))
        self.assertEqual( 10.0, c.convert("float", "10.0"))
        self.assertEqual( datetime.datetime(2018, 5, 7, 3, 1, 54),
                          c.convert("timestamp", "1525658514"))

        self.assertEqual( datetime.datetime(2018, 5, 25, 11, 30),
                          c.convert( "datetime", "11:30am 25-May-2018"))

        #see datetime.datetime.strptime for formatting
        #https://docs.python.org/3.6/library/datetime.html#datetime.datetime.strptime
        # 25-May-2018 : %d-%b-%Y
        # 11:30am : %I:%M%p
        self.assertEqual( datetime.datetime(2018, 5, 25, 11, 30),
                          c.convert_time( "datetime", "11:30am 25-May-2018", "%I:%M%p %d-%b-%Y"))
if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.test_autosplit']
    unittest.main()