"""
Created on 8 Aug 2017

@author: jdrumgoole
"""
import os
import unittest
from typing import List, Dict
from datetime import datetime

import pymongo

from pymongoimport.fieldfile import FieldFile
from pymongoimport.filewriter import FileWriter
from pymongoimport.filereader import FileReader
from pymongoimport.filesplitter import LineCounter
from pymongoimport.logger import Logger
from pymongoimport.type_converter import Converter
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
        self._db.drop_collection( "FC_TEST")

    def test_FieldConfig(self):
        fc = FieldFile(f("data/test_fieldconfig.tff"))
        self.assertEqual(len(fc.fields()), 4)

        self.assertEqual(fc.fields()[0], "Test 1")
        self.assertEqual(fc.fields()[3], "Test 4")

        fc = FieldFile(f("data/uk_property_prices.tff"))
        self.assertEqual(len(fc.fields()), 16)

        self.assertEqual(fc.fields()[0], "txn")
        self.assertEqual(fc.fields()[2], "Date of Transfer")
        self.assertEqual(fc.fields()[14], "PPD Category Type")

    def test_delimiter_no_header(self):
        start_count = self._col.count_documents({})
        fc = FieldFile(f("data/10k.tff"))
        parser = CSVParser(fc)
        reader = FileReader(f("data/10k.txt"), parser, has_header=False, delimiter="|")
        bw = FileWriter(self._col, reader)
        bw.write()
        self.assertEqual(self._col.count_documents({}) - start_count, 10000)

    def test_fieldfile_nomatch(self):
        fc = FieldFile(f("data/AandE_Data_2011-04-10.tff"))
        parser = CSVParser(fc)
        reader = FileReader(f('data/inventory.csv'), parser, has_header=True)
        bw = FileWriter(self._col, reader)
        with self.assertRaises(ValueError):
            bw.write()

    def test_new_delimiter_and_timeformat_header(self):
        start_count = self._col.count_documents({})
        fc = FieldFile(f("data/mot.tff"))
        parser = CSVParser(fc)
        reader = FileReader(f('data/mot_test_set_small.csv'), parser, has_header=False, delimiter="|")
        self.assertTrue( type(reader.name)==str)
        bw = FileWriter(self._col, reader)
        total = bw.write()
        lines = LineCounter(f('data/mot_test_set_small.csv')).line_count
        inserted_count = self._col.count_documents({}) - start_count
        self.assertEqual(inserted_count, total)
        self.assertEqual(inserted_count, lines)

    def test_delimiter_header(self):
        start_count = self._col.count_documents({})
        fc = FieldFile(f("data/AandE_Data_2011-04-10.tff"))
        parser = CSVParser(fc)
        reader = FileReader(f('data/AandE_Data_2011-04-10.csv'), parser, has_header=True)
        bw = FileWriter(self._col, reader)
        bw.write()
        self.assertEqual(self._col.count_documents({}) - start_count, 300)

    def test_generate_field_filename(self):
        fc = FieldFile.generate_field_file(f('data/inventory.csv'), ext="xx")
        self.assertEqual(fc.field_filename, f("data/inventory.xx"))
        os.unlink(fc.field_filename)

        fc = FieldFile.generate_field_file(f('data/inventory.csv'))
        self.assertEqual(fc.field_filename, f("data/inventory.tff"))
        os.unlink(fc.field_filename)

        fc = FieldFile.generate_field_file(f('data/inventory.csv.1'))
        self.assertEqual(fc.field_filename, f("data/inventory.csv.tff"), fc.field_filename)
        os.unlink(fc.field_filename)

        fc = FieldFile.generate_field_file(f('data/yellow_tripdata_2015-01-06-200k.csv.1'))
        self.assertEqual(fc.field_filename, f("data/yellow_tripdata_2015-01-06-200k.csv.tff"), fc.field_filename)
        os.unlink(fc.field_filename)

        fc = FieldFile.generate_field_file(f('data/yellow_tripdata_2015-01-06-200k.csv.10'))
        self.assertEqual(fc.field_filename, f("data/yellow_tripdata_2015-01-06-200k.csv.tff"), fc.field_filename)
        os.unlink(fc.field_filename)

        fc = FieldFile.generate_field_file(f('data/test_results_2016_10.txt.1'))
        self.assertEqual(fc.field_filename, f("data/test_results_2016_10.txt.tff"), fc.field_filename)
        os.unlink(fc.field_filename)

    def test_nyc_2016_genfieldfile(self):

        fc = FieldFile.generate_field_file(f('data/2018_Yellow_Taxi_Trip_Data_1000.csv'),
                                           delimiter=";")
        fc = FieldFile(fc.field_filename)


    def test_reader(self):
        fc = FieldFile.generate_field_file(f("data/inventory.csv"))
        ff = FieldFile(fc.field_filename)
        parser = CSVParser(ff)
        reader = FileReader(f("data/inventory.csv"), parser, has_header=True)
        for row in reader.read_file():
            for field in ff.fields():
                self.assertTrue(field in row)

        os.unlink(fc.field_filename)

        ff = FieldFile(f("data/uk_property_prices.tff"))
        csv_parser = CSVParser(field_file=ff)
        reader = FileReader(f("data/uk_property_prices.csv"), csv_parser, has_header=True)

        for row in reader.read_file():
            for field in ff.fields():
                if field == "txn": # converted to _id field
                    continue
                self.assertTrue(field in row, f"{field} not present")
                self.assertTrue(type(row["Price"]) == int)
                self.assertTrue(type(row["Date of Transfer"]) == datetime)

    def test_generate_fieldfile(self):
        fc = FieldFile.generate_field_file(f("data/inventory.csv"), ext="testff")
        self.assertEqual(fc.field_filename, f("data/inventory.testff"), fc.field_filename)
        self.assertTrue(os.path.isfile(f("data/inventory.testff")), f("data/inventory.testff"))
        parser = CSVParser(fc)
        reader = FileReader(f("data/inventory.csv"), parser, has_header=True)
        start_count = self._col.count_documents({})
        writer = FileWriter(self._col, reader)
        write_count=writer.write()
        line_count = LineCounter(f("data/inventory.csv")).line_count
        new_inserted_count = self._col.count_documents({}) - start_count
        self.assertEqual(new_inserted_count, write_count)  # header must be subtracted
        self.assertEqual(new_inserted_count, line_count - 1 ) # header must be subtracted
        os.unlink(f("data/inventory.testff"))

    def test_date(self):
        config = FieldFile(f("data/inventory_dates.tff"))
        parser = CSVParser(config, locator=False) # screws up comparison later if locator is true
        reader = FileReader(f("data/inventory.csv"),parser, has_header=True)
        raw_reader = FileReader(f("data/inventory.csv"),parser=None, has_header=True)
        start_count = self._col.count_documents({})
        writer = FileWriter(self._col, reader)
        docs_written = writer.write()
        line_count = LineCounter(f("data/inventory.csv")).line_count
        self.assertEqual(self._col.count_documents({}) - start_count, line_count - 1)  # header must be subtracted
        self.assertEqual(self._col.count_documents({}), docs_written)
        c = Converter()
        fields = config.fields()
        result_doc: Dict[str, object] = {}
        for row,raw_row in zip(reader.read_file(), raw_reader.read_file()):
            # print( row )
            for i,field in enumerate(fields):
                result_doc[field] = c.convert(config.type_value(field), raw_row[i])  # remember we type convert fields

            self.assertEqual(result_doc, row)

        db_doc = self._col.find_one(row)
        del db_doc["_id"]
        self.assertTrue(db_doc)
        self.assertEqual(db_doc, row)

    def testFieldDict(self):
        d = FieldFile(f("data/testresults.tff")).field_dict
        self.assertTrue("TestID" in d)
        self.assertTrue("FirstUseDate" in d)
        self.assertTrue("Colour" in d)
        self.assertTrue(d["TestID"]["type"] == "int")

    def test_duplicate_id(self):
        self.assertRaises(ValueError, FieldFile, f("data/duplicate_id.tff"))


if __name__ == "__main__":
    unittest.main()
