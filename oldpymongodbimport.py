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
import logging
import time


from fieldconfig import FieldConfig, FieldConfigException
import multiprocessing
    
class MongoDB( object ):
    
    def __init__(self, host, port, databaseName, collectionName, 
                 username=None, password=None, ssl=False ):
        self._host = host
        self._port = port
        self._databaseName = databaseName
        self._collectionName = collectionName
        self._database = None
        self._collection = None
        self._username = username
        self._password = password
        self._ssl = ssl
    
    def connect(self):
        self._client = pymongo.MongoClient( host=self._host, port=self._port, ssl=self._ssl)
        self._database = self._client[ self._databaseName]
        
        if self._username :
            if self._database.authenticate( self._username, self._password ):
#            if self._database.authenticate( self._username, self._password, mechanism='MONGODB-CR'):
                print( "successful login by %s (method SCRAM-SHA-1)" % self._username )
            else:
                print( "login failed for %s (method SCRAM-SHA-1)" % self._username )
                
        self._collection = self._database[ self._collectionName ]
        
    def collection(self):
        return self._collection

    def database(self):
        return self._database


class Reader( object ):
    
    def __init__(self, queue, fieldConfig, args, filename ):
        self._queue = queue
        self._fieldConfig = fieldConfig
        
        if self._fieldConfig is None :
            fieldFilename = makeFieldFilename( filename )
            try : 
                self._fieldConfig = FieldConfig( fieldFilename )
            except OSError, e:
                print( "Field file : '%s' for '%s' cannot be opened : %s" % ( fieldFilename, filename, e  ))
                print( "Ignoring : %s" % filename )
                raise FieldConfigException( "No field config file for %s" % filename )
        
    def fieldConfig(self):
        return self._fieldConfig
    
    def read(self, args, filename ):
        #print( "read")
            
        lineCount = 0
        with open( filename, "rb")  as f :
            reader = csv.DictReader( f, fieldnames = self._fieldConfig.fields(), delimiter = args.delimiter)
            for l in reader :
                lineCount = lineCount + 1 
                
                '''
                We send the fieldConfig with every line as the clients don't know which records are associated with which
                files. This is wasteful. We can fix up later TODO.
                '''
                self._queue.put(( l, self._fieldConfig ))
                
        return lineCount 

class Writer( object ):
    
    def __init__(self, queue ):
        
        self._queue = queue
        
    def write(self, args ):

        mdb = MongoDB(host=args.host, port=args.port, 
                      databaseName= args.database,
                      collectionName = args.collection,
                      username=args.username, 
                      password=args.password, 
                      ssl=args.ssl )
        
        mdb.connect()
        collection =mdb.collection()
        
        processing = True 
        bulker = None
        lineCount = 0 
        
        while  processing :
            
            if args.ordered :
                bulker = collection.initialize_ordered_bulk_op()
            else:
                bulker = collection.initialize_unordered_bulk_op()
                
            bulkerCount = 0
            
            for _ in range( args.chunksize ) :
                
                lineCount = lineCount + 1 
                
                #start = time.time()
                x = self._queue.get()
                #end = time.time()
                #print( "Queue waited : %f" % ( end - start ))
                
                if x is None :
                    processing = False
                    break 
                
                ( l, fieldConfig ) = x
                
                d = createDoc( fieldConfig, l, lineCount )

                bulker.insert( d )
                self._queue.task_done()
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
                d = createDoc( self._fieldConfig, dictEntry, lineCount )
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
            raise   
    
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

def multiProcessFiles( fieldConfig, args ):
    
    readers = []
    writers = []
    queue = multiprocessing.JoinableQueue( 10000 )
    stopEvent = multiprocessing.Event()

    
    try: 
        for i in args.filenames :
            try:
                r = Reader( queue, fieldConfig, args, i )
                readProc = multiprocessing.Process( name="reader : %s" % i, target= r.read, args=( args, i, ))
                readers.append( readProc )
                readProc.start()
            except FieldConfigException, e :
                print( "Field file issue processing: '%s'" % i )
                print( e )
            
        for i in range( args.multi ):
            w = Writer( queue )
            writeProc = multiprocessing.Process( name="writer: %i" % i, target= w.write, args=( args, ))
            writers.append( writeProc )
            writeProc.start()
                
        for i in readers:
            i.join()
            queue.put( None )
            
        for i in writers:
            i.join()


        
    except KeyboardInterrupt :
        for i in readers:
            i.terminate()
            i.join()
                
                
            for i in writers:
                i.terminate()
                i.join()
            
    pass


def processFiles( fieldConfig, args ):

    mdb = MongoDB(host=args.host, port=args.port, 
                  databaseName= args.database,
                  collectionName = args.collection,
                  username=args.username, 
                  password=args.password, 
                  ssl=args.ssl )
    mdb.connect()
    collection =mdb.collection()
    
    totalCount = 0
    results=[]
    failures=[]
    
    for i in args.filenames :
        if args.fieldfile is None :
            fieldFilename = makeFieldFilename( i )
            try : 
                fieldConfig= FieldConfig( fieldFilename )
            except OSError, e:
                print( "Field file : '%s' for '%s' cannot be opened : %s" % ( fieldFilename, i, e  ))
                print( "Ignoring : %s" % i )
                failures.append( i )
                continue

        print ("Processing : %s" % i )
        lineCount = 0 
        try :
            with open( i, "r") as f :
                lineCount = skipLines( f, args.skip )
                
                #print( "field names: %s" % fieldDict.keys() )
                reader = csv.DictReader( f, fieldnames = fieldConfig.fields(), delimiter = args.delimiter )
                colWriter = CollectionWriter( collection, args.ordered, fieldConfig, args.chunksize )
                while True :
                    result = colWriter.writeRecords( reader, lineCount )
                    lineCount = lineCount + result['nInserted']
                    totalCount = totalCount + result[ "nInserted" ]
                    if result[ "nInserted" ] < args.chunksize :
                        break
                    
                results.append( i )
                
        except OSError, e :
            print( "Can't open '%s' : %s" % ( i, e ))
            failures.append( i )
        except KeyboardInterrupt:
            print( "exiting...(ctrl-C ? )")
            sys.exit( 2 )
            
        print( "Processed: %s" % results )
        if len( failures ) > 0 :
            print( "Failed to process: %s" % failures )
            

def mainline( args ):
    
    
    '''
    >>> mainline( [ 'test_set_small.txt' ] )
    database: test, collection: test
    files ['test_set_small.txt']
    Processing : test_set_small.txt
    Completed processing : test_set_small.txt, (100 records)
    Processed test_set_small.txt
    '''
    
    parser = argparse.ArgumentParser(description='loader for MOT data', prog = "pyfastloader")
    parser.add_argument( '--database', default="test", help='specify the database name')
    parser.add_argument( '--collection', default="test", help='specify the collection name')
    parser.add_argument( '--host', default="localhost", help='hostname')
    parser.add_argument( '--port', default="27017", help='port name', type=int)
    parser.add_argument( '--username', default=None, help='username to login to database')
    parser.add_argument( '--password', default=None, help='password to login to database')
    parser.add_argument( '--ssl', default=False, action="store_true", help='use SSL for connections')
    parser.add_argument( '--chunksize', type=int, default=1000, help='set chunk size for bulk inserts' )
    parser.add_argument( '--skip', default=0, type=int, help="skip lines before reading")
    parser.add_argument( '--drop', default=False, action="store_true", help="drop collection before loading" )
    parser.add_argument( '--ordered', default=False, action="store_true", help="forced ordered inserts" )
    parser.add_argument( '--verbose', default=0, type=int, help="controls how noisy the app is" )
    parser.add_argument( "--fieldfile", default= None, type=str,  help="Field and type mappings")
    parser.add_argument( "--delimiter", default="|", type=str, help="The delimiter string used to split fields (default '|')")
    parser.add_argument( "--testmode", default=False, action="store_true", help="Run in test mode, no updates")
    parser.add_argument( "--multi", default=0, type=int, help="Run in multiprocessing mode")
    parser.add_argument( 'filenames', nargs="+", help='list of files')
    
    args= parser.parse_args( args )
    
    if args.testmode :
        import doctest
        doctest.testmod()
        sys.exit( 0 )
        

    filenames = args.filenames
    fieldConfig = None
    processedFiles = []
    
    if args.fieldfile:
        try :
            fieldConfig = FieldConfig( args.fieldfile )
        except OSError, e:
            print( "Field file : '%s' cannot be opened : %s" % (args.fieldfile, e  ))
    
    if args.drop :
        m = MongoDB( args.host, args.port, args.database, args.collection, args.username, args.password, ssl=args.ssl )
        m.connect()
        m.collection().drop()
        print( "dropped collection: %s.%s" % ( args.database, args.collection ))
    
    print(  "database: %s, collection: %s" % ( args.database, args.collection ))
    print( "files %s" % args.filenames )

    totalCount = 0
    if args.chunksize < 1 :
        print( "Chunksize must be 1 or more. Chunksize : %i" % args.chunksize )
        sys.exit( 1 )
    
    if args.multi :
        multiprocessing.log_to_stderr(logging.DEBUG )
        multiProcessFiles( fieldConfig, args )
    else:
        processFiles( fieldConfig, args )
        sys.exit(0)
        
#     for i in filenames:
#         print ("Processing : %s" % i )
# 
#         if args.fieldfile is None :
#             fieldFilename = makeFieldFilename( i )
#             try : 
#                 fieldConfig= FieldConfig( fieldFilename )
#             except OSError, e:
#                 print( "Field file : '%s' for '%s' cannot be opened : %s" % (args.fieldfile, i, e  ))
#                 print( "Ignoring : %s" % i )
#                 continue
#             
#             if args.multi > 0  :
#                 qList = []
# 
#                 for  j in range( args.multi ):
#                     q = multiprocessing.Queue( 10000 )
#                     qList.append( q )
#                     
#                 r = Reader( qList, fieldConfig, args.delimiter )
#                 readProc = multiprocessing.Process( target= r.read, args=( i, ))
#                 readProc.start()
#                 
#                 writePool = []
#                 for j in range( args.multi ):
#                     m = MongoDB( args.host, args.port, args.database, args.collection )
#                     w = Writer( m , qList[ j ], fieldConfig, args.ordered,  args.chunksize )
#                     writeProc = multiprocessing.Process( target= w.write, args=())
#                     writePool.append( writeProc )
#                     writeProc.start()
#                     
#                     
#                 readProc.join()
#                 for j in range( args.multi ):
#                     writePool[ j ].join()
#                     
#                 processedFiles.append( i )
#             else:
#                 processFiles( fieldConfig, args )
#                 mdb = MongoDB(host=args.host, port=args.port, 
#                               databaseName= args.database,
#                               collectionName = args.collection,
#                               username=args.username, 
#                               password=args.password, 
#                               ssl=args.ssl )
#                 mdb.connect()
#                 collection =mdb.collection()
#                 
#                 try:
#                     with open( i, "r" ) as f :
#         
#                         lineCount = skipLines( f, args.skip )
#                         
#                         #print( "field names: %s" % fieldDict.keys() )
#                         reader = csv.DictReader( f, fieldnames = fieldConfig.fields(), delimiter = args.delimiter )
#                         colWriter = CollectionWriter( collection, args.ordered, fieldConfig, args.chunksize )
#                         while True :
#                             result = colWriter.writeRecords( reader, lineCount )
#                             lineCount = lineCount + result['nInserted']
#                             totalCount = totalCount + result[ "nInserted" ]
#                             if result[ "nInserted" ] < args.chunksize :
#                                 break
#                             
#                         print( "Completed processing : %s, (%d records)" % ( i, totalCount ))
#                         processedFiles.append( i )
#                 except OSError, e :
#                     print( "Cannot process '%s' : %s" % ( i, e ))
#                     continue 
#         else:
#             print( "no such file : %s" % i ) 
#                 
#         for i in processedFiles :
#             print( "Processed %s" % i )
    
    
if __name__ == '__main__':
    
    sys.argv.pop( 0 ) # remove script file name 
    mainline( sys.argv )