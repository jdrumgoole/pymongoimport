import unittest

from pymongo_import.config_file import Config_File
class Test(unittest.TestCase):

    def test_Config_File(self):

        cfg = Config_File( "data/10k.ff")
        self.assertTrue( "test_id" in cfg.fields())
        self.assertTrue( "cylinder_capacity" in cfg.fields())

        self.assertEqual( cfg.type_value( "test_id"), "int")
        self.assertEqual( cfg.type_value( "test_date"), "datetime")

    def test_property_prices(self):
        cfg = Config_File( "data/uk_property_prices.ff")
        self.assertTrue( cfg.hasNewName( "txn"))
        self.assertFalse( cfg.name_value( "txn") is None)

if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.test_fileprocessor']
    unittest.main()