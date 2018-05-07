import unittest

from pymongo_import.config_file import Config_File
class Test(unittest.TestCase):

    def test_Config_File(self):

        cfg = Config_File( "data/10k.ff")

        self.assertTrue( "TestID" in cfg.fields())
        self.assertTrue( "CylinderCapacity" in cfg.fields())

        self.assertEqual( cfg.type_value( "TestID"), "int")
        self.assertEqual( cfg.type_value( "TestDate"), "date")

if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.test_fileprocessor']
    unittest.main()