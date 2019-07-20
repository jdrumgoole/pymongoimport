"""
Created on 8 Aug 2017

@author: jdrumgoole
"""
import os
import unittest

import pymongo

from pymongoimport.fieldfile import FieldFile
from pymongoimport.filewriter import FileWriter
from pymongoimport.filesplitter import LineCounter
from pymongoimport.logger import Logger
from pymongoimport.type_converter import Converter
from pymongoimport.configfile import ConfigFile
from pymongoimport.csvparser import CSVParser

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
        fc = ConfigFile(f("data/test_fieldconfig.ff"))
        self.assertEqual(len(fc.fields()), 4)

        self.assertEqual(fc.fields()[0], "Test 1")
        self.assertEqual(fc.fields()[3], "Test 4")

        fc = ConfigFile(f("data/uk_property_prices.ff"))
        self.assertEqual(len(fc.fields()), 15)

        self.assertEqual(fc.fields()[0], "txn")
        self.assertEqual(fc.fields()[2], "Date of Transfer")
        self.assertEqual(fc.fields()[14], "PPD Category Type")

    def test_delimiter_no_header(self):
        start_count = self._col.count_documents({})
        fc = ConfigFile(f("data/10k.ff"))
        parser = CSVParser(fc, hasheader=False, delimiter="|")
        bw = FileWriter(self._col, parser)
        bw.insert_file(f("data/10k.txt"))
        self.assertEqual(self._col.count_documents({}) - start_count, 10000)

    def test_delimiter_header(self):
        start_count = self._col.count_documents({})
        fc = ConfigFile(f("data/AandE_Data_2011-04-10.ff"))
        parser = CSVParser(fc, hasheader=True)
        bw = FileWriter(self._col, parser)
        bw.insert_file(f("data/AandE_Data_2011-04-10.csv"))
        self.assertEqual(self._col.count_documents({}) - start_count, 300)

    def test_generate_field_filename(self):
        fc = FieldFile(f('data/inventory.csv'), ext="xx")
        fc.generate_field_file()
        self.assertEqual(fc.field_filename, f("data/inventory.xx"))
        os.unlink(fc.field_filename)

        fc = FieldFile(f('data/inventory.csv'))
        fc.generate_field_file()
        self.assertEqual(fc.field_filename, f("data/inventory.ff"))
        os.unlink(fc.field_filename)

        fc = FieldFile(f('data/inventory.csv.1'))
        fc.generate_field_file()
        self.assertEqual(fc.field_filename, f("data/inventory.ff"), fc.field_filename)
        os.unlink(fc.field_filename)

        fc = FieldFile(f('data/yellow_tripdata_2015-01-06-200k.csv.1'))
        fc.generate_field_file()
        self.assertEqual(fc.field_filename, f("data/yellow_tripdata_2015-01-06-200k.ff"), fc.field_filename)
        os.unlink(fc.field_filename)

        fc = FieldFile(f('data/yellow_tripdata_2015-01-06-200k.csv.10'))
        fc.generate_field_file()
        self.assertEqual(fc.field_filename, f("data/yellow_tripdata_2015-01-06-200k.ff"), fc.field_filename)
        os.unlink(fc.field_filename)

        fc = FieldFile(f('data/test_results_2016_10.txt.1'))
        fc.generate_field_file()
        self.assertEqual(fc.field_filename, f("data/test_results_2016_10.ff"), fc.field_filename)
        os.unlink(fc.field_filename)

    def test_dict_reader(self):
        fc = FieldFile(f("data/inventory.csv"))
        fc.generate_field_file()
        cfg = ConfigFile(fc.field_filename)
        csv_parser = CSVParser(cfg, hasheader=True, delimiter=',')
        with open(f("data/inventory.csv"), "r") as file:
            if csv_parser.hasheader():
                _ = file.readline()
            reader = csv_parser.get_dict_reader(file)
            for row in reader:
                for field in cfg.fields():
                    self.assertTrue(field in row)

        os.unlink(fc.field_filename)

        cfg = ConfigFile(f("data/uk_property_prices.ff"))
        csv_parser = CSVParser(cfg, hasheader=False, delimiter=",")
        with open(f("data/uk_property_prices.csv"), "r") as file:
            if csv_parser.hasheader():
                _ = file.readline()
            reader = csv_parser.get_dict_reader(file)
            for row in reader:
                for field in cfg.fields():
                    self.assertTrue(field in row)
                    self.assertTrue(type(row["Price"]) == str)
                    self.assertTrue(type(row["Date of Transfer"]) == str)

    def test_generate_fieldfile(self):
        fc = FieldFile(f("data/inventory.csv"), ext="testff")
        self.assertEqual(fc.field_filename, f("data/inventory.testff"), fc.field_filename)
        fc.generate_field_file()
        self.assertTrue(os.path.isfile(f("data/inventory.testff")), f("data/inventory.testff"))
        config = ConfigFile(fc.field_filename)
        csv_parser = CSVParser(config, hasheader=True, delimiter=",")
        start_count = self._col.count_documents({})
        writer = FileWriter(self._col, csv_parser)
        writer.insert_file(f("data/inventory.csv"))
        line_count = LineCounter(f("data/inventory.csv")).line_count
        self.assertEqual(self._col.count_documents({}) - start_count, line_count - 1)  # header must be subtracted

        os.unlink(f("data/inventory.testff"))

        c = Converter()
        fc = FieldFile(f("data/inventory.csv"))
        fc.generate_field_file()
        cfg = ConfigFile(fc.field_filename)
        parser = CSVParser(cfg, hasheader=True)
        with open(f("data/inventory.csv"), "r")  as file:
            if parser.hasheader():
                _ = file.readline()
            reader = parser.get_dict_reader(file)
            fields = config.fields()
            for row in reader:
                # print( row )
                for field in fields:
                    row[field] = c.convert(config.type_value(field), row[field])  # remember we type convert fields

                doc = self._col.find_one(row)
                self.assertTrue(doc)

    def test_date(self):
        config = ConfigFile(f("data/inventory_dates.ff"))
        parser = CSVParser(config, hasheader=True)
        start_count = self._col.count_documents({})
        writer = FileWriter(self._col, parser)
        writer.insert_file(f("data/inventory.csv"))
        line_count = LineCounter(f("data/inventory.csv")).line_count
        self.assertEqual(self._col.count_documents({}) - start_count, line_count - 1)  # header must be subtracted

        c = Converter()

        with open(f("data/inventory.csv"), "r") as file:
            if parser.hasheader():
                _ = file.readline()
            reader = parser.get_dict_reader(file)
            fields = config.fields()
            for row in reader:
                # print( row )
                for field in fields:
                    row[field] = c.convert(config.type_value(field), row[field])  # remember we type convert fields

                doc = self._col.find_one(row)
                self.assertTrue(doc)

    def testFieldDict(self):
        d = ConfigFile(f("data/testresults.ff")).fieldDict()
        self.assertTrue("TestID" in d)
        self.assertTrue("FirstUseDate" in d)
        self.assertTrue("Colour" in d)
        self.assertTrue(d["TestID"]["type"] == "int")

    def test_duplicate_id(self):
        self.assertRaises(ValueError, ConfigFile, f("data/duplicate_id.ff"))


if __name__ == "__main__":
    unittest.main()
