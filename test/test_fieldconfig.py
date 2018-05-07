'''
Created on 8 Aug 2017

@author: jdrumgoole
'''
import unittest
import os
import datetime

import pymongo

from pymongo_import.fieldconfig import FieldConfig, FieldConfigException
from pymongo_import.logger import Logger
from pymongo_import.file_writer import File_Writer
from pymongo_import.filesplitter import File_Splitter, Line_Counter
from pymongo_import.type_converter import Converter

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
        fc = FieldConfig("data/test_fieldconfig.ff")
        cfg = fc.config()
        self.assertEqual(len(cfg.fields()), 4)

        self.assertEqual(cfg.fields()[0], "Test 1")
        self.assertEqual(cfg.fields()[3], "Test 4")

        fc = FieldConfig("data/uk_property_prices.ff")
        cfg = fc.config()
        self.assertEqual(len(cfg.fields()), 15)

        self.assertEqual(cfg.fields()[0], "txn")
        self.assertEqual(cfg.fields()[2], "Date of Transfer")
        self.assertEqual(cfg.fields()[14], "PPD Category Type")

    def test_delimiter_no_header(self):
        start_count = self._col.count()
        fc = FieldConfig("data/10k.ff", delimiter='|', hasheader=False)
        bw = File_Writer(self._col, fc)
        bw.insert_file("data/10k.txt")
        self.assertEqual(self._col.count() - start_count, 10000)

    def test_delimiter_header(self):
        start_count = self._col.count()
        fc = FieldConfig("data/AandE_Data_2011-04-10.ff", delimiter=',', hasheader=True)
        bw = File_Writer(self._col, fc)
        bw.insert_file("data/AandE_Data_2011-04-10.csv")
        self.assertEqual(self._col.count() - start_count, 300)

    def test_generate_field_filename(self):
        fc_filename = FieldConfig.generate_field_filename('data/inventory.csv')
        self.assertEqual(fc_filename, "data/inventory.ff", fc_filename)
        fc_filename = FieldConfig.generate_field_filename('data/inventory.csv', ext="xx")
        self.assertEqual(fc_filename, "data/inventory.xx")
        fc_filename = FieldConfig.generate_field_filename('data/inventory.csv', ext=".xx")
        self.assertEqual(fc_filename, "data/inventory.xx")
        fc_filename = FieldConfig.generate_field_filename('data/inventory.csv')
        self.assertEqual(fc_filename, "data/inventory.ff")

        fc_filename = FieldConfig.generate_field_filename('data/inventory.csv.1')
        self.assertEqual(fc_filename, "data/inventory.ff", fc_filename)

        fc_filename = FieldConfig.generate_field_filename('data/yellow_tripdata_2015-01-06-200k.csv.1')
        self.assertEqual(fc_filename, "data/yellow_tripdata_2015-01-06-200k.ff", fc_filename)

        fc_filename = FieldConfig.generate_field_filename('data/yellow_tripdata_2015-01-06-200k.csv.10')
        self.assertEqual(fc_filename, "data/yellow_tripdata_2015-01-06-200k.ff", fc_filename)

        fc_filename = FieldConfig.generate_field_filename('test_result_2016.txt.1')
        self.assertEqual(fc_filename, "test_result_2016.ff", fc_filename)

    def test_dict_reader(self):
        fc_filename = FieldConfig.generate_field_file("data/inventory.csv")
        fc = FieldConfig(fc_filename)
        cfg = fc.config()
        with open("data/inventory.csv", "r")  as f:
            if fc.hasheader():
                _ = f.readline()
            reader = fc.get_dict_reader(f)
            for row in reader:
                for field in cfg.fields():
                    self.assertTrue(field in row)

        fc = FieldConfig("data/uk_property_prices.ff")
        cfg = fc.config()
        with open("data/uk_property_prices.csv", "r")  as f:
            if fc.hasheader():
                _ = f.readline()
            reader = fc.get_dict_reader(f)
            for row in reader:
                for field in cfg.fields():
                    self.assertTrue(field in row)
                    self.assertTrue(type(row["Price"]) == str)
                    self.assertTrue(type(row["Date of Transfer"]) == str)

    def test_generate_fieldfile(self):

        fc_filename = FieldConfig.generate_field_file("data/inventory.csv", ext="testff")
        self.assertTrue(os.path.isfile("data/inventory.testff"))
        fc = FieldConfig(fc_filename, hasheader=True)
        config = fc.config()
        start_count = self._col.count()
        writer = File_Writer(self._col, fc)
        writer.insert_file("data/inventory.csv")
        line_count = Line_Counter("data/inventory.csv").count()
        self.assertEqual(self._col.count() - start_count, line_count - 1)  # header must be subtracted

        os.unlink("data/inventory.testff")

        c = Converter()
        with open("data/inventory.csv", "r")  as f:
            if fc.hasheader():
                _ = f.readline()
            reader = fc.get_dict_reader(f)
            fields = config.fields()
            for row in reader:
                # print( row )
                for f in fields:
                    row[f] = c.convert(config.type_value(f), row[f])  # remember we type convert fields

                doc = self._col.find_one(row)
                self.assertTrue(doc)

    def test_date(self):
        fc = FieldConfig("data/inventory_dates.ff", hasheader=True)
        config = fc.config()
        start_count = self._col.count()
        writer = File_Writer(self._col, fc)
        writer.insert_file("data/inventory.csv")
        line_count = Line_Counter("data/inventory.csv").count()
        self.assertEqual(self._col.count() - start_count, line_count - 1)  # header must be subtracted

        c = Converter()

        with open("data/inventory.csv", "r")  as f:
            if fc.hasheader():
                _ = f.readline()
            reader = fc.get_dict_reader(f)
            fields = config.fields()
            for row in reader:
                # print( row )
                for f in fields:
                    row[f] = c.convert(config.type_value(f), row[f])  # remember we type convert fields

                doc = self._col.find_one(row)
                self.assertTrue(doc)

    def test_field_config_exception(self):

        # f.open( "duplicateID.ff" )
        self.assertRaises(OSError, FieldConfig, "nosuchfile.ff")
        # print( "fields: %s" % f.fields())

    def testFieldDict(self):
        f = FieldConfig("data/testresults.ff", delimiter="|")
        d = f.config().field_dict()
        self.assertTrue("TestID" in d )
        self.assertTrue("FirstUseDate" in d)
        self.assertTrue("Colour" in d)
        self.assertTrue(d["TestID"]["type"] == "int")

    def test_duplicate_id(self):
        self.assertRaises(ValueError, FieldConfig, "data/duplicate_id.ff")


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.test_FieldConfig']
    unittest.main()
