"""
Created on 8 Aug 2017

@author: jdrumgoole
"""
import os
import unittest

import pymongo

from pymongoimport.fieldconfig import FieldConfig
from pymongoimport.file_writer import File_Writer
from pymongoimport.filesplitter import LineCounter
from pymongoimport.logger import Logger
from pymongoimport.type_converter import Converter

path_dir = os.path.dirname(os.path.realpath(__file__))


def f(path):
    return os.path.join(path_dir, path)


class Test(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(Test, self).__init__(*args, **kwargs)
        Logger.add_null_hander()

    def setUp(self):
        self._client = pymongo.MongoClient(host="mongodb://localhost:27017")
        self._db = self._client["FC_TEST"]
        self._col = self._db["FC_TEST"]

    def tearDown(self):
        # self._db.drop_collection( "FC_TEST")
        pass

    def test_FieldConfig(self):
        fc = FieldConfig(None, f("data/test_fieldconfig.ff"))
        cfg = fc.config()
        self.assertEqual(len(cfg.fields()), 4)

        self.assertEqual(cfg.fields()[0], "Test 1")
        self.assertEqual(cfg.fields()[3], "Test 4")

        fc = FieldConfig(None, f("data/uk_property_prices.ff"))
        cfg = fc.config()
        self.assertEqual(len(cfg.fields()), 15)

        self.assertEqual(cfg.fields()[0], "txn")
        self.assertEqual(cfg.fields()[2], "Date of Transfer")
        self.assertEqual(cfg.fields()[14], "PPD Category Type")

    def test_delimiter_no_header(self):
        start_count = self._col.count_documents({})
        fc = FieldConfig(None, f("data/10k.ff"), delimiter='|', hasheader=False)
        bw = File_Writer(self._col, fc)
        bw.insert_file(f("data/10k.txt"))
        self.assertEqual(self._col.count_documents({}) - start_count, 10000)

    def test_delimiter_header(self):
        start_count = self._col.count_documents({})
        fc = FieldConfig(None, f("data/AandE_Data_2011-04-10.ff"), delimiter=',', hasheader=True)
        bw = File_Writer(self._col, fc)
        bw.insert_file(f("data/AandE_Data_2011-04-10.csv"))
        self.assertEqual(self._col.count_documents({}) - start_count, 300)

    def test_generate_field_filename(self):
        fc_filename = FieldConfig.generate_field_filename(f('data/inventory.csv'))
        self.assertEqual(fc_filename, f("data/inventory.ff"), fc_filename)
        fc_filename = FieldConfig.generate_field_filename(f('data/inventory.csv'), ext="xx")
        self.assertEqual(fc_filename, f("data/inventory.xx"))
        fc_filename = FieldConfig.generate_field_filename(f('data/inventory.csv'), ext=".xx")
        self.assertEqual(fc_filename, f("data/inventory.xx"))
        fc_filename = FieldConfig.generate_field_filename(f('data/inventory.csv'))
        self.assertEqual(fc_filename, f("data/inventory.ff"))

        fc_filename = FieldConfig.generate_field_filename(f('data/inventory.csv.1'))
        self.assertEqual(fc_filename, f("data/inventory.ff"), fc_filename)

        fc_filename = FieldConfig.generate_field_filename(f('data/yellow_tripdata_2015-01-06-200k.csv.1'))
        self.assertEqual(fc_filename, f("data/yellow_tripdata_2015-01-06-200k.ff"), fc_filename)

        fc_filename = FieldConfig.generate_field_filename(f('data/yellow_tripdata_2015-01-06-200k.csv.10'))
        self.assertEqual(fc_filename, f("data/yellow_tripdata_2015-01-06-200k.ff"), fc_filename)

        fc_filename = FieldConfig.generate_field_filename(f('test_result_2016.txt.1'))
        self.assertEqual(fc_filename, f("test_result_2016.ff"), fc_filename)

    def test_dict_reader(self):
        fc_filename = FieldConfig.generate_field_file(f("data/inventory.csv"))
        fc = FieldConfig(None, fc_filename)
        cfg = fc.config()
        with open(f("data/inventory.csv"), "r") as file:
            if fc.hasheader():
                _ = file.readline()
            reader = fc.get_dict_reader(file)
            for row in reader:
                for field in cfg.fields():
                    self.assertTrue(field in row)

        fc = FieldConfig(None, f("data/uk_property_prices.ff"))
        cfg = fc.config()
        with open(f("data/uk_property_prices.csv"), "r") as file:
            if fc.hasheader():
                _ = file.readline()
            reader = fc.get_dict_reader(file)
            for row in reader:
                for field in cfg.fields():
                    self.assertTrue(field in row)
                    self.assertTrue(type(row["Price"]) == str)
                    self.assertTrue(type(row["Date of Transfer"]) == str)

    def test_generate_fieldfile(self):
        fc_filename = FieldConfig.generate_field_file(f("data/inventory.csv"), ext="testff")
        self.assertTrue(os.path.isfile(f("data/inventory.testff")))
        fc = FieldConfig(None, fc_filename, hasheader=True)
        config = fc.config()
        start_count = self._col.count_documents({})
        writer = File_Writer(self._col, fc)
        writer.insert_file(f("data/inventory.csv"))
        line_count = LineCounter(f("data/inventory.csv")).line_count
        self.assertEqual(self._col.count_documents({}) - start_count, line_count - 1)  # header must be subtracted

        os.unlink(f("data/inventory.testff"))

        c = Converter()
        with open(f("data/inventory.csv"), "r")  as file:
            if fc.hasheader():
                _ = file.readline()
            reader = fc.get_dict_reader(file)
            fields = config.fields()
            for row in reader:
                # print( row )
                for field in fields:
                    row[field] = c.convert(config.type_value(field), row[field])  # remember we type convert fields

                doc = self._col.find_one(row)
                self.assertTrue(doc)

    def test_date(self):
        fc = FieldConfig(None, f("data/inventory_dates.ff"), hasheader=True)
        config = fc.config()
        start_count = self._col.count_documents({})
        writer = File_Writer(self._col, fc)
        writer.insert_file(f("data/inventory.csv"))
        line_count = LineCounter(f("data/inventory.csv")).line_count
        self.assertEqual(self._col.count_documents({}) - start_count, line_count - 1)  # header must be subtracted

        c = Converter()

        with open(f("data/inventory.csv"), "r") as file:
            if fc.hasheader():
                _ = file.readline()
            reader = fc.get_dict_reader(file)
            fields = config.fields()
            for row in reader:
                # print( row )
                for field in fields:
                    row[field] = c.convert(config.type_value(field), row[field])  # remember we type convert fields

                doc = self._col.find_one(row)
                self.assertTrue(doc)

    def test_field_config_exception(self):

        # f.open( "duplicateID.ff" )
        self.assertRaises(OSError, FieldConfig, None, "nosuchfile.ff")
        # print( "fields: %s" % f.fields())

    def testFieldDict(self):
        fc = FieldConfig(None, f("data/testresults.ff"), delimiter="|")
        d = fc.config().field_dict()
        self.assertTrue("TestID" in d)
        self.assertTrue("FirstUseDate" in d)
        self.assertTrue("Colour" in d)
        self.assertTrue(d["TestID"]["type"] == "int")

    def test_duplicate_id(self):
        self.assertRaises(ValueError, FieldConfig, None, f("data/duplicate_id.ff"))


if __name__ == "__main__":
    unittest.main()
