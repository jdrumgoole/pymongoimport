'''
Created on 19 Feb 2016

@author: jdrumgoole
'''
from __future__ import print_function

import pymongo
from datetime import datetime
import Queue
import csv
import sys
import threading
import time


class recordReader( threading.Thread ):
    
    def __init__(self, q, chunksize, filename ) :
        threading.Thread.__init__(self)
        self._q = q 
        self._filename = filename 
        
        self._fieldNames = [ "TestID", "VehicleID", "TestDate", "TestClassID", 
                       "TestType", "TestResult", "Test Mileage", "Postcode", 
                       "Make", "Model", "Colour","FuelType", "CylinderCapacity", "FirstUseDate" ]
        

        self._chunkSize = chunksize
        self._count = 0
        
    def run(self):
        #print( "Running RecordReader")
        with open( filename, "r") as f :
            reader = csv.DictReader( f, fieldnames = self._fieldNames, delimiter = '|')
            for r in reader :
                r["TestDate"] = datetime.strptime( r["TestDate"],"%Y-%m-%d")
                self._q.put( r )
                time.sleep( 0.0001 )
                self._count = self._count + 1
                #print( "Read %d records" % self._count )
                
        self._q.put( { "None" : None } )
            
    def count(self):
        return self._count 
        
class recordWriter( threading.Thread ):
    
    def __init__(self, q, e, chunkSize, collection ):
        threading.Thread.__init__(self)
        self._q = q 
        self._chunkSize = chunkSize 
        self._collection = collection
        self._count = 0
        self._e = e

    def run(self):
        #print( "Running RecordWriter" )
        while self._e.is_set() :
            ( result, stopValue ) = writeFromQChunk( self._collection, self._e, self._q, self._chunkSize)
            
            
            if ( result is None ) :
                continue
            elif ( "None" in stopValue ) and ( stopValue[ "None" ]  is None ) :
                #print( "Exiting writeLoop : StopValue is None" )
                self._count =self._count + result[ 'nInserted' ]
                #print( "Inserted %d records" % self._count )
            
                break
            else:
                self._count =self._count + result[ 'nInserted' ]
                #print( "Inserted %d records" % self._count )
            

#
# Read from the input queue until no items (indicated by timeout and or stopValue.
# Then write the collected items if any are pending.
#
def writeFromQChunk( col, e, q, chunkSize ):
    bulker = col.initialize_ordered_bulk_op()
    
    #print( "Running writeFromQChunk")
    r = { "None" : "Continue" }
    pendingWrites = 0
    try :
        for _ in range( chunkSize ) :
            r = q.get( timeout = 0.0001 )
            if "None" in r :
                q.task_done()
                e.clear() # signal other writer threads to stop
            
                break ;
            else:
                #print( "Inserting")
                bulker.insert( r )
                pendingWrites = pendingWrites + 1 
                q.task_done()
                r = { "None" : "Continue" }
            
    except Queue.Empty :
        #print( "Queue empty" )
        pass
     
    if ( pendingWrites > 0 ):   
        result = bulker.execute()
        return ( result, r )
    else:
        return ( None, r )
     
def tracker(totalCount, result ):
    totalCount = totalCount + result[ "nInserted" ]
    print("Inserted %d records" % totalCount )
    return totalCount 

def writeChunk( d, chunkSize ):
    bulker = db.mot_results.initialize_ordered_bulk_op()
    count = 0
    for r in d :
        r["TestDate"] = datetime.strptime( r["TestDate"],"%Y-%m-%d")
        bulker.insert( r )
        count = count + 1 
        if ( count == chunkSize ):
            break
    
    return bulker.execute()

def writerThreads( count, q, chunkSize, collection ): 
    
    threads = []
    runEvent = threading.Event() ;
    runEvent.set()
    
    for i in range( count ) :
        threads.append( recordWriter( q, runEvent, chunkSize, collection ))
        threads[ i ].start()
        
    for i in range( count ):
        threads[i].join()
       
def printRecord( names, rec, end="\n" ):
    print ("{",)
    for name in fieldNames :
        print ( "   %s = %s, " %  ( name, rec[ name ] ), end=end)
    print( "}")
 
if __name__ == '__main__':
    
    mc = pymongo.MongoClient()
    db = mc[ "vosa"]
    filename = None
    collection = db.mot_results
    fieldNames = [ "TestID", "VehicleID", "TestDate", "TestClassID", 
                  "TestType", "TestResult", "Test Mileage", "Postcode", 
                  "Make", "Model", "Colour","FuelType", "CylinderCapacity", "FirstUseDate" ]
    if len( sys.argv ) > 1 :
        filename = sys.argv[ 1 ]
    else:
        print("No arguments to program" )
        sys.exit( 1 )
        
    useThreading= True
    
    if useThreading :
        chunkSize = 1000
        q = Queue.Queue()
        reader = recordReader( q, chunkSize, filename )
        reader.start()
        writerThreads( 1, q, chunkSize, collection )
        reader.join()
    else: 
        totalCount = 0
        bulkerSize = 20000
        with open( filename, "r") as f :
            reader = csv.DictReader( f, fieldnames = fieldNames, delimiter = '|')
            while True :
                result = writeChunk( reader, bulkerSize )
                totalCount = tracker( totalCount, result )
                if result[ "nInserted" ] < bulkerSize :
                    break
            
            
#         result = writeChunk(reader, remainder )
#         totalCount = tracker( totalCount, result )
#         for row in reader:
#             row[ "TestDate"] = datetime.strptime( row[ "TestDate"],"%Y-%m-%d")
#             result = bulker.insert( row )
#             totalCount = totalCount + result[ "nInserted" ]
#             if ( InputRecordsize - totalCount ) 
#             totalCount = totalCount + 1 
#             bulkCount = bulkCount + 1 
#             if (( inputRecordSize - remaintotalCount ) > remainder )
#             else ( bulkCount == bulkerSize ) :
#                 result = bulker.execute()
#                 bulker = db.mot_results.initialize_ordered_bulk_op()
#                 print( result )
#                 bulkCount = 0 
#                 totalCount = totalCount + result[ "nInserted" ]
#                 print("Inserted %d records" % totalCount )
                
            
 
            # printRecord( fieldNames, row )
            #sys.exit( 1 )
        