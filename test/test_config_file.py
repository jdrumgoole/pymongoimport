import os
import unittest

from pymongoimport.config_file import Config_File, dict_to_fields

path_dir = os.path.dirname(os.path.realpath(__file__))


def f(path):
    return os.path.join(path_dir, path)


class Test(unittest.TestCase):

    def test_Config_File(self):
        cfg = Config_File(f("data/10k.ff"))
        self.assertTrue("test_id" in cfg.fields())
        self.assertTrue("cylinder_capacity" in cfg.fields())

        self.assertEqual(cfg.type_value("test_id"), "int")
        self.assertEqual(cfg.type_value("test_date"), "datetime")

    def test_property_prices(self):
        cfg = Config_File(f("data/uk_property_prices.ff"))
        self.assertTrue(cfg.hasNewName("txn"))
        self.assertFalse(cfg.name_value("txn") is None)

    def test_dict_to_fields(self):
        a = {"a": 1, "b": 2, "c": 3}
        b = {"w": 5, "z": a}
        c = {"m": a, "n": b}

        fields = dict_to_fields(a)
        self.assertEqual(len(fields), 3)
        self.assertEqual(["a", "b", "c"], fields)

        fields = dict_to_fields(b)
        self.assertEqual(len(fields), 4)
        self.assertEqual(["w", "a", "b", "c"], fields)

        fields = dict_to_fields(c)
        self.assertEqual(len(fields), 7)
        self.assertEqual(["a", "b", "c", "w", "a", "b", "c"], fields)

    # def test_dict_walker(self):
    #
    #     d1 = {"a":1 }
    #     paths = dict_walker(d1)
    #     self.assertEqual(paths, ["a"])
    #
    #     d2 = {"a": 1, "b":2}
    #     paths = dict_walker(d2)
    #     self.assertEqual(paths, ["a", "b"])
    #
    #     d3 = {"a": 1, "b":2, "c": { "d" : 3, "e" : 4 }}
    #     paths = dict_walker(d3)
    #     self.assertEqual(paths, ["a", "b", "c.d", "c.e"], paths)


if __name__ == "__main__":
    unittest.main()
