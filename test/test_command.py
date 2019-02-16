import unittest
import os

import pymongo
from pymongo_import.command import Drop_Command, Generate_Fieldfile_Command, Import_Command
from pymongo_import.audit import Audit
from logging import getLogger
from pymongo_import.filesplitter import Line_Counter
import shutil

class Test(unittest.TestCase):

    def setUp(self):
        self._client = pymongo.MongoClient()
        self._database = self._client[ "TEST_CMD"]
        self._collection = self._database[ "test"]
        self._collection.insert_one( { "hello" : "world"})

    def tearDown(self):
        #self._client.drop_database( "TEST_CMD")
        pass

    def test_Drop_Command(self):

        self._audit = Audit(database=self._client[ "TEST_AUDIT"])
        id = self._audit.start_batch( { "test" : "test_batch"})

        cmd = Drop_Command( log=getLogger(__file__), database=self._database, audit=self._audit, id=id)

        self.assertTrue( self._collection.find_one({ "hello" : "world"} ))

        cmd.run("test")

        self.assertFalse( self._collection.find_one({ "hello" : "world"} ))

        self._audit.end_batch(id)

    def test_Generate_Fieldfile_Command(self):

        cmd = Generate_Fieldfile_Command( log=getLogger(__file__), delimiter=",")
        shutil.copy( "data/yellow_tripdata_2015-01-06-200k.csv", "data/test_generate_ff.csv")
        cmd.run("data/test_generate_ff.csv")
        self.assertTrue( os.path.isfile( "data/test_generate_ff.ff"))
        os.unlink("data/test_generate_ff.ff")
        os.unlink("data/test_generate_ff.csv")

    def test_Import_Command(self):

        self._audit = Audit(database=self._client[ "TEST_AUDIT"])
        id = self._audit.start_batch( { "test" : "test_batch"})
        collection = self._database[ "import_test"]

        start_size = collection.count_documents({})
        size_10k = Line_Counter( "data/10k.txt").line_count()
        size_120 = Line_Counter( "data/120lines.txt").line_count()
        cmd = Import_Command(log=getLogger(__file__),
                             audit=self._audit,
                             id=id,
                             collection=collection,
                             field_filename="data/10k.ff",
                             delimiter="|",
                             hasheader=False,
                             onerror="warn",
                             limit=0)

        cmd.run( "data/10k.txt", "data/120lines.txt")
        new_size = collection.count_documents({})
        self.assertEqual( size_10k + size_120, new_size - start_size )

        self._audit.end_batch(id)
if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.test_FieldConfig']
    unittest.main()
