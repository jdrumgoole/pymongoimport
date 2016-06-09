'''
Created on 19 Feb 2016

Need tests for skip larger than file size.

@author: jdrumgoole
'''
from __future__ import print_function

import pymongo
from datetime import datetime
from collections import OrderedDict

import csv
import os
import argparse
import sys
import signal

from fieldconfig import FieldConfig
import multiprocessing



    
class MongoDB( object ):
    
    def __init__(self, host, port, databaseName, collectionName ):
        self._host = host
        self._port = port
        self._databaseName = databaseName
        self._collectionName = collectionName
        self._database = None
        self._collection = None
    
    def connect(self):
        self._client = pymongo.MongoClient( host=self._host, port=self._port )
        self._database = self._client[ self._databaseName]
        self._collection = self._database[ self._collectionName ]
        
    def collection(self):
        return self._collection

    def database(self):
        return self._database

class Reader( object ):
    
    def __init__(self, qList, stopEvent, fieldDict, delimiter):
        self._queue = qList
        self._fieldDict = fieldDict
        self._delimiter = delimiter
        self._qList = qList
        self._stopEvent = stopEvent
        
    def read(self, filename ):
        #print( "read")
        qIndex = 0 
        with open( filename, "rb")  as f :
            reader = csv.DictReader( f, fieldnames = self._fieldDict.keys(), delimiter = self._delimiter)
            for l in reader :
                #print( l )
                self._qList[ qIndex ].put( l )
                qIndex = qIndex + 1 
                if  qIndex == len( self._qList ):
                    qIndex = 0
                if self._stopEvent.is_set() :
                    break
                
        self._queue.put( None )
                
class Writer( object ):
    
    def __init__(self, mongo, queue, stopEvent, fieldDict, orderedWrites, chunkSize ):
        
        self._mongo = mongo
        self._queue = queue
        self._chunkSize = chunkSize
        self._fieldDict = fieldDict
        self._orderedWrites = orderedWrites 
        self._stopEvent = stopEvent
        
    def write(self):
        
        self._mongo.connect()
        collection = self._mongo.collection()
        
        processing = True 
        bulker = None
        lineCount = 0 
        
        while  processing :
            
            if self._orderedWrites :
                bulker = collection.initialize_ordered_bulk_op()
            else:
                bulker = collection.initialize_unordered_bulk_op()
                
            bulkerCount = 0
            
            for _ in range( self._chunkSize ) :

                if self._stopEvent.is_set() :
                    processing = False
                    break
                
                lineCount = lineCount + 1 
                l = self._queue.get()
                
                if l is None :
                    processing = False
                    break 
                
                d = createDoc( self._fieldDict, l, lineCount )

                bulker.insert( d )
                bulkerCount = bulkerCount + 1 
                
            if ( bulkerCount > 0 ) :
                try: 
                    bulker.execute()
                except pymongo.errors.BulkWriteError as e :
                    print( "Bulk write error : %s" % e )
                    raise
        
                

def skipLines( f, skipCount ):
    '''
    >>> f = open( "test_set_small.txt", "r" )
    >>> skipLines( f , 20 )
    20
    '''
    
    lineCount = 0 
    if ( skipCount > 0 ) :
        #print( "Skipping")
        dummy = f.readline()
        while dummy :
            lineCount = lineCount + 1
            if ( lineCount == skipCount ) :
                break ;
            dummy = f.readline()
            
    return lineCount 

def makeFieldFilename( filename ):
    '''
    >>> makeFieldFilename( "test.txt" ) 
    'test.ff'
    >>> makeFieldFilename( "test" )
    'test.ff'
    '''
    ( x, _ ) = os.path.splitext( filename )
    return x + ".ff" 

class CollectionWriter(object):
    
    def __init__(self, collection, orderedWrites, fieldDict, chunkSize ):
        
        self._collection = collection
        self._orderedWrites = orderedWrites
        self._fieldDict = fieldDict
        self._chunkSize = chunkSize
        
    def writeRecords(self, reader, lineCount ):
        bulker = None
        try : 
            if self._orderedWrites :
                bulker = self._collection.initialize_ordered_bulk_op()
            else:
                bulker = self._collection.initialize_unordered_bulk_op()
                
            bulkerCount = 0
            for dictEntry in reader :
                #print( "dict: %s" % dictEntry )
                lineCount = lineCount + 1 
                d = createDoc( self._fieldDict, dictEntry, lineCount )
                bulker.insert( d )
                bulkerCount = bulkerCount + 1 
                if ( bulkerCount == self._chunkSize ):
                    break
            
            if ( bulkerCount > 0 ) :
                return bulker.execute()
            else:
                return { "nInserted" : 0 }

        except pymongo.errors.BulkWriteError as e :
            print( "Bulk write error : %s" % e )
            raise
        
        
from  pprint import pprint

def createDoc( fieldDict, dictEntry, lineCount):

    doc = OrderedDict()
    

    for k in fieldDict :
        try :
            #print( "line: %s" % line )
            if fieldDict[ k ]["type" ] == "_id" :
                doc[ "_id" ] = int( dictEntry[ k ] )
                #print ( "id : (int : %i), (str : %s) " % ( int(line[ k ]), line[ k ] ))
                #pprint( line )
            elif fieldDict[ k ]["type" ] == "str" :
                doc[ k ] = dictEntry[ k ]
            elif fieldDict[ k ]["type"] == "int" :
                doc[ k ] = int( dictEntry[ k ])
            elif fieldDict[ k ]["type" ] == "date" :
                if dictEntry[ k ] == "NULL" :
                    doc[ k ] = "NULL"
                else:
                    doc[ k ] = datetime.strptime( dictEntry[ k ], fieldDict[ k ][ "format"] )
                    
        except ValueError :
            print( "Value error parsing field : [%s]" % k )
            print( "read value is: '%s'" % dictEntry[ k ] )
            print( "line: %i, '%s'" % ( lineCount, dictEntry ))
            #print( "ValueError parsing filed : %s with value : %s (type of field: $s) " % ( str(k), str(line[ k ]), str(fieldDict[ k]["type"])))
            raise ;
    
    
    return doc
        
def tracker(totalCount, result, filename ):
    totalCount = totalCount + result[ "nInserted" ]
    #print("Inserted %d records from %s" % ( totalCount, filename ))
    return totalCount 


       
# def printRecord( names, rec, end="\n" ):
#     print ("{",)
#     for name in fieldNames :
#         print ( "   %s = %s, " %  ( name, rec[ name ] ), end=end)
#     print( "}")
#  


ProcessManager = multiprocessing.Manager()
stopEvent = ProcessManager.Event()

    
def mainline( args ):
    
    global ProcessManager
    global stopEvent
    
    '''
    >>> mainline( [ 'test_set_small.txt' ] )
    database: test, collection: test
    files ['test_set_small.txt']
    Processing : test_set_small.txt
    Completed processing : test_set_small.txt, (100 records)
    Processed test_set_small.txt
    '''
    
    parser = argparse.ArgumentParser(description='loader for MOT data', prog = "pyfastloader")
    parser.add_argument('--database', default="test", help='specify the database name')
    parser.add_argument('--collection', default="test", help='specify the collection name')
    parser.add_argument('--host', default="localhost", help='hostname')
    parser.add_argument('--port', default="27017", help='port name', type=int)
    parser.add_argument('--chunksize', default="5000", help='set chunk size for bulk inserts', type=int)
    parser.add_argument('filenames', nargs="*", help='list of files')
    parser.add_argument( '--skip', default=0, type=int, help="skip lines before reading")
    parser.add_argument( '--drop', default=False, action="store_true", help="drop collection before loading" )
    parser.add_argument( '--ordered', default=False, action="store_true", help="forced ordered inserts" )
    parser.add_argument( '--verbose', default=0, type=int, help="controls how noisy the app is" )
    parser.add_argument( "--fieldfile", default= None, type=str,  help="Field and type mappings")
    parser.add_argument( "--delimiter", default="|", type=str, help="The delimiter string used to split fields (default '|')")
    parser.add_argument( "--testmode", default=False, action="store_true", help="Run in test mode, no updates")
    parser.add_argument( "--multi", default=0, type=int, help="Run in multiprocessing mode")
    args= parser.parse_args( args )
    
    if args.testmode :
        import doctest
        doctest.testmod()
        sys.exit( 0 )
        

    filenames = args.filenames

    fc = FieldConfig()
    
    if args.drop :
        m = MongoDB( args.host, args.port, args.database, args.collection )
        m.connect()
        m.collection().drop()
        print( "dropped collection: %s.%s" % ( args.database, args.collection ))
    
    print(  "database: %s, collection: %s" % ( args.database, args.collection ))
    print( "files %s" % args.filenames )
#     fieldNames = [ "TestID", "VehicleID", "TestDate", "TestClassID", 
#                   "TestType", "TestResult", "Test Mileage", "Postcode", 
#                   "Make", "Model", "Colour","FuelType", "CylinderCapacity", "FirstUseDate" ]
    

    fieldDict = OrderedDict()
    processedFiles = []
    if args.fieldfile is None :
        pass # process later in file processing loop
    elif os.path.isfile( args.fieldfile ):
        fieldDict = fc.read( args.fieldfile )
        
    #print( "FieldDict : %s " % fieldDict )
    totalCount = 0
    if ( args.chunksize > 0 ) :
        bulkerSize = args.chunksize 
    else:
        bulkerSize = 5000
    
    for i in filenames:
        print ("Processing : %s" % i )

        if args.fieldfile is None :
            fieldFilename = makeFieldFilename( i )
            if os.path.isfile( fieldFilename ):
                fieldDict = fc.read( fieldFilename )
            else:
                print( "No valid field file defined for '%s'" % i )
                continue
            
        if ( os.path.exists( i ) and os.path.isfile( i )) :
            
            if args.multi > 0  :
                
                stopEvent.clear()
                qList = []

                for  j in range( args.multi ):
                    q = multiprocessing.Queue( 10000 )
                    qList.append( q )
                    
                r = Reader( qList, stopEvent, fieldDict, args.delimiter )
                readProc = multiprocessing.Process( target= r.read, args=( i, ))
                readProc.start()
                
                writePool = []
                for j in range( args.multi ):
                    m = MongoDB( args.host, args.port, args.database, args.collection )
                    w = Writer( m , qList[ j ], stopEvent, fieldDict, args.ordered,  args.chunksize )
                    writeProc = multiprocessing.Process( target= w.write, args=())
                    writePool.append( writeProc )
                    writeProc.start()
                    
                    
                readProc.join()
                for j in range( args.multi ):
                    writePool[ j ].join()
                    
                processedFiles.append( i )
            else:
                mc = pymongo.MongoClient( host=args.host, port=args.port )
                db = mc[ args.database ]
                collection = db[ args.collection ]
                with open( i, "r" ) as f :
    
                    lineCount = skipLines( f, args.skip )
                    
                    #print( "field names: %s" % fieldDict.keys() )
                    reader = csv.DictReader( f, fieldnames = fieldDict.keys(), delimiter = args.delimiter )
                    colWriter = CollectionWriter( collection, args.ordered, fieldDict, bulkerSize )
                    while True :
                        result = colWriter.writeRecords( reader, lineCount )
                        lineCount = lineCount + result['nInserted']
                        totalCount = totalCount + result[ "nInserted" ]
                        if result[ "nInserted" ] < bulkerSize :
                            break
                        
                    print( "Completed processing : %s, (%d records)" % ( i, totalCount ))
                    processedFiles.append( i )
                    
        else:
            print( "no such file : %s" % i ) 
                
        for i in processedFiles :
            print( "Processed %s" % i )


def signalHandler(signal, frame  ):
    global stopEvent
    print( "Caught CTRL-C, exiting")
    stopEvent.set()
    
    
if __name__ == '__main__':
    
    signal.signal( signal.SIGINT, signalHandler )
    sys.argv.pop( 0 ) # remove script file name 
    mainline( sys.argv )