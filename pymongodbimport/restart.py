'''
Created on 30 Jul 2017

@author: jdrumgoole
'''
from datetime import datetime
class Restarter(object):
    '''
    Track insertion of a collection of docs by adding the last inserted
    ID into a collection called "restartlog". Each time we insert we add
    a doc with a timestamp and an ID field and a count of the number of 
    entries inserted to date. The doc also contains a batch
    start time.
    
    These class assumes the object ID is defined by default as per the MongoDB docs
    (https://docs.mongodb.com/manual/reference/method/ObjectId/). In this case in a single run 
    of the pymongoimport the IDs will contain identical host and process components. We can use 
    these fields to identify inserts that happened in the previous run. So we search for all inserts
    with an ID greater than the ID in the restartlog. 
    
    We then scan that list of inserts for a matching ID
    (remember we may be running multiple batch uploads in parallel) to find the inserts related to this 
    batch restart. Once we have that list of matching inserts then we have the count of objects inserted.
    Now we know where the restart position should be (restartlog number of entries + len( list of matching inserts)
    We can now skip() to that position in the input file and update the restart log.

    '''


    def __init__(self, collection, name, database ):
        '''
        Constructor
        '''
        self._restartlog = database[ "restartlog" ]
        self._collection = collection
        self._name = name
        self._restartDoc = self._restartlog.find_one( { "name" : self._name })
        
        if self._restartDoc is None :
            
            self._restartDoc = self._restartlog.insert_one( { "name"   : self._name, 
                                                              "start"  : datetime.utcnow(),
                                                              "count"  : 0 }  )
        
        
    @staticmethod
    def splitID( doc_id ):
        '''
        Split a MongoDB Object ID
        a 4-byte value representing the seconds since the Unix epoch,
        a 3-byte machine identifier,
        a 2-byte process id, and
        A 3-byte counter, starting with a random value.
        '''
        id_str     = str( doc_id )
        #        epoch 0        machine   1    process ID  2    counter 3
        return ( id_str[ 0:8 ],id_str[ 8:14],id_str[ 14:18], id_str[ 18:24]  )
    
    def update(self, doc_id, count ):
        
        self._restartlog.find_one_and_update( { "name"      : self._name },
                                              { "$inc"      : { "count" : count },
                                                "timestamp" : datetime.utcnow(),
                                                "doc_id"    : doc_id })
        
    def restart(self ):
        
        self._restartDoc = self._restartlog.find_one( { "name" : self._name })
        count = self._restartDoc[ "count"]
        ( epoch, machine, pid, counter ) = Restarter.splitID( self._restartDoc[ "doc_id"])
        cursor = self._collection.find( { "_id" : { "$gt" : self._restartDoc[ "doc_id" ]}})
        
        for i in cursor:
            ( _, i_machine, i_pid, _ ) = Restarter.splitID( i[ "_id"])
              
            if i_machine == machine and i_pid == pid :
                count = count + 1
                
        return count
            