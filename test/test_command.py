import os
import shutil
import unittest
from logging import getLogger

import pymongo

from pymongoimport.audit import Audit
from pymongoimport.command import Drop_Command, Generate_Fieldfile_Command, Import_Command
from pymongoimport.filesplitter import LineCounter

path_dir = os.path.dirname(os.path.realpath(__file__))


def f(path):
    return os.path.join(path_dir, path)


class Test(unittest.TestCase):

    def setUp(self):
        self._client = pymongo.MongoClient()
        self._database = self._client["TEST_CMD"]
        self._collection = self._database["test"]
        self._collection.insert_one({"hello": "world"})

    def tearDown(self):
        # self._client.drop_database( "TEST_CMD")
        pass

    def test_Drop_Command(self):
        self._audit = Audit(database=self._client["TEST_AUDIT"])
        batch_id = self._audit.start_batch({"test": "test_batch"})

        cmd = Drop_Command(log=getLogger(__file__),
                           database=self._database,
                           audit=self._audit,
                           id=batch_id)

        self.assertTrue(self._collection.find_one({"hello": "world"}))

        cmd.run("test")

        self.assertFalse(self._collection.find_one({"hello": "world"}))

        self._audit.end_batch(batch_id)

    def test_Generate_Fieldfile_Command(self):
        cmd = Generate_Fieldfile_Command(log=getLogger(__file__), delimiter=",")
        shutil.copy(f("data/yellow_tripdata_2015-01-06-200k.csv"),
                    f("data/test_generate_ff.csv"))
        cmd.run(f("data/test_generate_ff.csv"))
        self.assertTrue(os.path.isfile(f("data/test_generate_ff.ff")))
        os.unlink(f("data/test_generate_ff.ff"))
        os.unlink(f("data/test_generate_ff.csv"))

    def test_Import_Command(self):
        self._audit = Audit(database=self._client["TEST_AUDIT"])
        batch_id = self._audit.start_batch({"test": "test_batch"})
        collection = self._database["import_test"]

        start_size = collection.count_documents({})
        size_10k = LineCounter(f("data/10k.txt")).line_count
        size_120 = LineCounter(f("data/120lines.txt")).line_count
        cmd = Import_Command(log=getLogger(__file__),
                             audit=self._audit,
                             id=batch_id,
                             collection=collection,
                             field_filename=f("data/10k.ff"),
                             delimiter="|",
                             hasheader=False,
                             onerror="warn",
                             limit=0)

        cmd.run(f("data/10k.txt"), f("data/120lines.txt"))
        new_size = collection.count_documents({})
        self.assertEqual(size_10k + size_120, new_size - start_size)

        self._audit.end_batch(batch_id)


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.test_FieldConfig']
    unittest.main()
