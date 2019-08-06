'''
Created on 31 Jul 2017

@author: jdrumgoole
'''
import unittest


class Test(unittest.TestCase):
    pass

    #
    # def setUp(self):
    #     self._client = pymongo.MongoClient()
    #     self._client.drop_database( "RESTART_TEST")
    #     self._db = self._client[ "RESTART_TEST"]
    #     self._collection = self._db[ "RESTART_TEST"]
    #     self._root = Root()
    #
    # def tearDown(self):
    #     pass #self._client.drop_database( "RESTART_TEST")
    #
    # def get_last_doc(self, col ):
    #     last_doc = col.find().sort([ ("_id", pymongo.DESCENDING )]).limit( 1 )
    #
    #     for i in last_doc :
    #         return i
    #
    # def test_Restart(self):
    #     fc = FieldFile( self._root.root_path( "data", "10k.ff"), has_header=False, delimiter="|")
    #     bw = FileWriter( self._collection, fc)
    #     bw.write( self._root.root_path( "data", "10k.txt"), restart=True )
    #     audit = self._db[ "audit"]
    #     self.assertEqual( audit.count(), 1 )
    #     audit_doc = audit.find_one()
    #
    #     self.assertEqual( audit_doc[ "state" ], "completed")
    #
    #     last_doc = self.get_last_doc( self._collection )
    #     self.assertEqual( audit_doc[ "last_doc_id"], last_doc[ "_id"])
    #     self.assertEqual( audit_doc[ "count"], 10000 )
    #
    #     bw.write( self._root.root_path( "data", "10k.txt"), restart=True )
    #
    #
    #     self.assertEqual( audit.count(), 2 )
    #     last_audit_doc = self.get_last_doc( audit )
    #
    #     self.assertEqual( last_audit_doc[ "state" ], "completed")
    #
    #     last_doc = self.get_last_doc( self._collection )
    #
    #     self.assertEqual( last_audit_doc[ "last_doc_id"], last_doc[ "_id"])
    #     self.assertEqual( last_audit_doc[ "count"], 10000 )


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.test_Restart']
    unittest.main()
