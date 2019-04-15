import unittest
import os

from pymongo_import.config_file import Config_File, dict_to_fields

path_dir = os.path.dirname(os.path.realpath(__file__))


def f(path):
    return os.path.join(path_dir, path)


class Test(unittest.TestCase):

    def test_Config_File(self):

        cfg = Config_File(f("data/10k.ff"))
        self.assertTrue( "test_id" in cfg.fields())
        self.assertTrue( "cylinder_capacity" in cfg.fields())

        self.assertEqual( cfg.type_value( "test_id"), "int")
        self.assertEqual( cfg.type_value( "test_date"), "datetime")

    def test_property_prices(self):
        cfg = Config_File(f("data/uk_property_prices.ff"))
        self.assertTrue(cfg.hasNewName("txn"))
        self.assertFalse(cfg.name_value("txn") is None)

    def test_dict_to_fields(self):

        a={ "a" : 1, "b" : 2, "c" : 3}
        b={ "w" : 5, "z" : a}

        fields = dict_to_fields(a, [])
        self.assertEqual(len(fields), 3)

        fields = dict_to_fields(b, [])
        self.assertEqual(len(fields), 5)


if __name__ == "__main__":
    unittest.main()
