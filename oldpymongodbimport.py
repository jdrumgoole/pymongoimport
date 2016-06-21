'''
Created on 19 Feb 2016

Import data from a CSV file with type information.

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
    '''
    Open a file and read the contents one line at a time. Post each line to a list of queues in 
    round robin order.
    
    '''
    def __init__(self, fieldConfig, delimiter):
        self._delimiter = delimiter
        self._fieldConfig = fieldConfig 
        
    def read(self, filename, chunkSize, skip=0 ):
        lines = []
        with open( filename, "rb")  as f :
            skipLines( f, skip )
            reader = csv.DictReader( f, fieldnames = self._fieldConfig.fields(), delimiter = self._delimiter)
            return self.buffer( reader, chunkSize )
        
    def buffer(self, reader, chunkSize  ):
        i = 0
        buf = []
        for l in reader:
            buf.append( l )
            i = i + 1 
            if ( i == chunkSize ):
                break
            
        return LineBuffer( buf )
            
        
    def qRead(self, qList, filename ):
        '''
        Round robin puts to queues in qList.
        '''
        #print( "read")
        qIndex = 0 
        for l in self.read( filename ):
            #print( l )
            qList[ qIndex ].put( l )
            qIndex = qIndex + 1 
            if  qIndex == len( qList ):
                qIndex = 0
            
        for i in qList :    
            i.put( None )
                

class LineBuffer( object ):
    
    def __init__(self, buf ):
        self._buf = buf
        
    def buf(self):

    
class Writer( object ):
    '''
    Read each line from a queue, write using the bulk writer.
    
    '''
    
    def __init__(self, collection, fieldConfig, queue, orderedWrites=False, chunkSize=1000 ):
        
        self._collection = collection
        self._chunkSize = chunkSize
        self._fieldConfig = fieldConfig
        self._orderedWrites = orderedWrites 
        
    def write(self, lineCount, lines ):
        if self._orderedWrites :
            bulker = self._collection.initialize_ordered_bulk_op()
        else:
            bulker = self._collection.initialize_unordered_bulk_op()
                
        bulkerCount = 0
       
        for i,j in lines :
            lineCount = lineCount + 1 
           
            if i is None :
                break 
           
            d = createDoc( self._fieldConfig, i, lineCount )

            bulker.insert( d )
            bulkerCount = bulkerCount + 1 
           
        try: 
            bulker.execute()
        except pymongo.errors.BulkWriteError as e :
            print( "Bulk write error : %s" % e )
            raise

        return lineCount
   
    def qWrite(self, collection, lineCount, queue ):
        
        lines = []
        itemCount = 0
        while True:
            item = queue.get()
            
            if item is None:
                break
            else:
                itemCount = itemCount + 1 
                lines.append( item )
                if itemCount == self._chunkSize :
                    lineCount = self.write( collection, lineCount, lines )
                    itemCount = 0
                    lines = []
                

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
    
    def __init__(self, collection, orderedWrites, fieldConfig, chunkSize ):
        
        self._collection = collection
        self._orderedWrites = orderedWrites
        self._fieldConfig = fieldConfig
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

def createDoc( fieldConfig, dictEntry, lineCount):

    doc = OrderedDict()
    

    for k in fieldConfig.fields() :
        print( k )
        print( dictEntry )
        try :
            #print( "line: %s" % line )
            
            typeField = fieldConfig.typeData( k )
            if typeField == "int" :
                v = int( dictEntry[ k ])
            elif typeField == "str" :
                v = str( dictEntry[ k ])
            elif typeField == "date":
                if dictEntry[ k ] == "NULL" :
                    v = 'NULL'
                else:
                    v = datetime.strptime( dictEntry[ k ], fieldConfig.formatData( k ) )
                    
            if fieldConfig.hasNewName( k ):
                doc[ fieldConfig.nameData( k )] = v
            else:
                doc[ k ] = v
                    
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
    
    parser = argparse.ArgumentParser(description='loader for MOT data', prog = "pymongodbimport")
    parser.add_argument('--database', default="test", help='specify the database name')
    parser.add_argument('--collection', default="test", help='specify the collection name')
    parser.add_argument('--host', default="localhost", help='hostname')
    parser.add_argument('--port', default="27017", help='port name', type=int)
    parser.add_argument('--chunksize', default="5000", help='set chunk size for bulk inserts', type=int)
    parser.add_argument('filenames', nargs="+", help='list of files')
    parser.add_argument( '--skip', default=0, type=int, help="skip lines before reading")
    parser.add_argument( '--drop', default=False, action="store_true", help="drop collection before loading" )
    parser.add_argument( '--ordered', default=False, action="store_true", help="forced ordered inserts" )
    parser.add_argument( '--verbose', default=0, type=int, help="controls how noisy the app is" )
    parser.add_argument( "--fieldfile", default="fieldfile.ff", type=str,  help="Field and type mappings")
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
    

    fieldConfig = None
    processedFiles = []
    if args.fieldfile is None :
        pass # process later in file processing loop
    elif os.path.isfile( args.fieldfile ):
        fieldConfig = FieldConfig( args.fieldfile )
        
    #print( "FieldDict : %s " % fieldDict )
    totalCount = 0
    if ( args.chunksize > 0 ) :
        bulkerSize = args.chunksize 
    else:
        bulkerSize = 5000
    
    for i in filenames:
        print ("Processing : %s" % i )

        if fieldConfig is None :
            fieldFilename = makeFieldFilename( i )
            if os.path.isfile( fieldFilename ):
                fieldConfig = FieldConfig( fieldFilename )
            else:
                print( "No valid field file defined for '%s'" % i )
                fieldConfig = None # reset for next iteration of loop
                continue
            
        if ( os.path.exists( i ) and os.path.isfile( i )) :
            
            if args.multi > 0  :
            

                    
                
                    
                for 
                qList = []

                for  j in range( args.multi ):
                    q = multiprocessing.Queue( 10000 )
                    qList.append( q )
                    
                r = Reader( qList, fieldConfig, args.delimiter )
                readProc = multiprocessing.Process( target= r.read, args=( i, ))
                readProc.start()
                
                writePool = []
                for j in range( args.multi ):
                    m = MongoDB( args.host, args.port, args.database, args.collection )
                    w = Writer( m , qList[ j ], stopEvent, fieldConfig, args.ordered,  args.chunksize )
                    writeProc = multiprocessing.Process( target= w.write, args=())
                    writePool.append( writeProc )
                    writeProc.start()
                    
                    
                readProc.join()
                for j in range( args.multi ):
                    writePool[ j ].join()
                    
                processedFiles.append( i )
                fieldConfig = None
            else:
                
                mdb = MongoDB( host=args.host, port=args.port, databaseName=args.database, collectionName=args.collection)
                mdb.connect()
                reader = Reader( fieldConfig, args.delimiter )
                
                for linesBuffer in reader.read( i, args.chunksize, args.skip ) :
                    lineCount = args.skip
                    writer = Writer( mdb.collection(), fieldConfig, args.ordered, bulkerSize )
                    while True :
                        result = writer.write( lineCount, linesBuffer )
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

    
    
if __name__ == '__main__':

    sys.argv.pop( 0 ) # remove script file name 
    mainline( sys.argv )

        