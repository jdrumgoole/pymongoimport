# noinspection PyUnresolvedReferences
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
from mongodb import MongoDB




class xReader( object ):
    
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
        
        try :
            mdb.connect()
        except pymongo.errors.ServerSelectionTimeoutError, e :
            print( "server %s on port %i timed out during connection: %s" % ( args.host, args.port, e.details ))
            sys.exit( 2 )
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
    ( x, _ ) = os.path.splitext( os.path.basename( filename ))
    return x + ".ff" 

class BatchWriter(object):
     
    def __init__(self, collection, orderedWrites, fieldConfig, chunkSize ):
         
        self._collection = collection
        self._orderedWrites = orderedWrites
        self._fieldConfig = fieldConfig
        self._chunkSize = chunkSize
         
    def insertWrite(self, reader, lineCount):
        
        lineBuffer = []
        bufferSize = 0
        for line in reader :
            d = createDoc( self._fieldConfig, line, lineCount )
            lineBuffer.insert( bufferSize, d )
            #print( buffer )
            bufferSize = bufferSize + 1
            if bufferSize == self._chunkSize :
                self._collection.insert_many( lineBuffer[0:bufferSize] )
                lineCount = lineCount + bufferSize
                bufferSize = 0
                   
        if bufferSize > 0 :
            self._collection.insert_many( lineBuffer[ 0:bufferSize ])
            lineCount = lineCount + bufferSize
            bufferSize = 0
    
        return lineCount

 
        
    def bulkWrite(self, reader, lineCount ):
        bulker = None
        
        try : 
            if self._orderedWrites :
                bulker = self._collection.initialize_ordered_bulk_op()
            else:
                bulker = self._collection.initialize_unordered_bulk_op()
                
            timeStart = time.time() 
            bulkerCount = 0
            insertedThisQuantum = 0
            for dictEntry in reader :
                timeNow = time.time()
                if timeNow > timeStart + 1  :
                    print( "Inserted %i records per second, record number: %i" % ( insertedThisQuantum, lineCount ))
                    insertedThisQuantum = 0
                    timeStart = timeNow
        
                
                #print( "dict: %s" % dictEntry )
                lineCount = lineCount + 1 
                d = createDoc( self._fieldConfig, dictEntry, lineCount )
                bulker.insert( d )
                bulkerCount = bulkerCount + 1 
                if ( bulkerCount == self._chunkSize ):
                    bulker.execute()
                    if self._orderedWrites :
                        bulker = self._collection.initialize_ordered_bulk_op()
                    else:
                        bulker = self._collection.initialize_unordered_bulk_op()
                        
                    insertedThisQuantum = insertedThisQuantum + bulkerCount 
                    bulkerCount = 0
             
            if ( bulkerCount > 0 ) :
                bulker.execute()

 
        except pymongo.errors.BulkWriteError as e :
            print( "Bulk write error : %s" % e.details )
            raise
        
        return  lineCount 

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
    
    importerProcs = []
    writers = []
    queue = multiprocessing.JoinableQueue( 10000 )
    stopEvent = multiprocessing.Event()

    
    try: 
        for i in args.filenames :
            importerProc = multiprocessing.Process( name="reader : %s" % i, target= processOneFile, args=( fieldConfig, args, i, ))
            importerProcs.append( importerProc )
            importerProc.start()
                
        for p in importerProcs :
            p.join()
        
    except KeyboardInterrupt :
        for p in importerProcs :
            p.terminate()
            p.join()

class InputFileException(Exception):
    def __init__(self, *args,**kwargs):
        Exception.__init__(self,*args,**kwargs)

def processOneFile( fieldConfig, args, filename ):

    try :
        mdb = MongoDB(host=args.host, port=args.port, 
                      databaseName= args.database,
                      collectionName = args.collection,
                      username=args.username, 
                      password=args.password, 
                      ssl=args.ssl )

        mdb.connect()
        collection =mdb.collection()
        
    except pymongo.errors.ServerSelectionTimeoutError, e :
            print( "Exception: server %s on port %i timed out during connection: %s" % ( args.host, args.port, e.details ))
            sys.exit( 2 )
            
    except pymongo.errors.OperationFailure, e :
        print( "Exception: Operations Failure: %s" % e.details )
        sys.exit( 2 )
        
    if args.fieldfile is None :
        fieldFilename = makeFieldFilename( filename )
        try : 
            fieldConfig= FieldConfig( fieldFilename )
        except OSError, e:
            raise FieldConfigException( "no valid field file for :%s"  % filename )


    if args.restart:
        skip = collection.count()
    else:
        skip = args.skip
        
    print ("Processing : %s" % filename )
    lineCount = 0 
    try :
        with open( filename, "r") as f :
            lineCount = skipLines( f, skip )
            

            #print( "field names: %s" % fieldDict.keys() )
            reader = csv.DictReader( f, fieldnames = fieldConfig.fields(), delimiter = args.delimiter )
            
            bw = BatchWriter( collection, False, fieldConfig, args.chunksize )
            if args.insertmany:
                lineCount = bw.insertWrite( reader, lineCount)
            else:
                lineCount = bw.bulkWrite(reader, lineCount)
            return ( filename, lineCount )
            
    except OSError, e :
        raise InputFileException( "Can't open '%s' : %s" % ( filename, e ))
    
    except KeyboardInterrupt:
        print( "Keyboard Interrupt exiting...")
        sys.exit( 2 )
        

    
def processFiles( fieldConfig, args ):
    
    totalCount = 0
    lineCount = 0
    results=[]
    failures=[]
    
    for i in args.filenames :
        try:
            ( filename, lineCount ) = processOneFile( fieldConfig, args, i )
            totalCount = lineCount + totalCount
            results.append( i )
        except FieldConfigException, e :
            print( "Field file error for %s : %s" % ( i, e ))
            failures.append( i )
        except InputFileException, e :
            print( "Input file error for %s : %s" % ( i, e ))
            failures.append( i )
            
    if len( results ) > 0 :
        print( "Processed         : %s" % results )
    if len( failures ) > 0 :
        print( "Failed to process : %s" % failures )
        

def mainline( args ):
    
    __VERSION__ = "1.1"
    '''
    >>> mainline( [ 'test_set_small.txt' ] )
    database: test, collection: test
    files ['test_set_small.txt']
    Processing : test_set_small.txt
    Completed processing : test_set_small.txt, (100 records)
    Processed test_set_small.txt
    '''
    
    usage_message = '''
    
    pymongodbimport is a python program that will import data into a mongodb
    database (default 'test' ) and a mongodb collection (default 'test' ).
    
    The input files must be named using the --filenames parameter.
    
    Each file in the input list must correspond to a fieldfile format that is
    common across all the files. The fieldfile is specified by the 
    --fieldfile parameter.
    
    An example run:
    
    python pymongodbimport.py --database demo --collection demo --fieldfile test_set_small.ff test_set_small.txt
    '''
    parser = argparse.ArgumentParser( prog = "pymongodbimport", usage=usage_message )
    parser.add_argument( '--database', default="test", help='specify the database name')
    parser.add_argument( '--collection', default="test", help='specify the collection name')
    parser.add_argument( '--host', default="localhost", help='hostname')
    parser.add_argument( '--port', default="27017", help='port name', type=int)
    parser.add_argument( '--username', default=None, help='username to login to database')
    parser.add_argument( '--password', default=None, help='password to login to database')
    parser.add_argument( '--admindb', default="admin", help="Admin database used for authentication" )
    parser.add_argument( '--ssl', default=False, action="store_true", help='use SSL for connections')
    parser.add_argument( '--chunksize', type=int, default=1000, help='set chunk size for bulk inserts' )
    parser.add_argument( '--skip', default=0, type=int, help="skip lines before reading")
    parser.add_argument( '--restart', default=False, action="store_true", help="use record count to skip")
    parser.add_argument( '--insertmany', default=False, action="store_true", help="use insert_many")
    parser.add_argument( '--testlogin', default=False, action="store_true", help="test database login")
    parser.add_argument( '--drop', default=False, action="store_true", help="drop collection before loading" )
    parser.add_argument( '--ordered', default=False, action="store_true", help="forced ordered inserts" )
    parser.add_argument( '--verbose', default=0, type=int, help="controls how noisy the app is" )
    parser.add_argument( "--fieldfile", default= None, type=str,  help="Field and type mappings")
    parser.add_argument( "--delimiter", default="|", type=str, help="The delimiter string used to split fields (default '|')")
    parser.add_argument( "--testmode", default=False, action="store_true", help="Run in test mode, no updates")
    parser.add_argument( "--multi", default=0, type=int, help="Run in multiprocessing mode")
    parser.add_argument( 'filenames', nargs="*", help='list of files')
    parser.add_argument('--version', action='version', version='%(prog)s version:' + __VERSION__ )
    
    args= parser.parse_args( args )
    
    if args.testmode :
        import doctest
        doctest.testmod()
        sys.exit( 0 )
        

    filenames = args.filenames
    fieldConfig = None
    processedFiles = []
    
    logger = logging.getLogger( args.database )
    
    if args.testlogin:
        try: 
            m = MongoDB( args.host, args.port, args.database, args.collection, args.username, args.password, ssl=args.ssl, admindb=args.admindb )
            m.connect(source=args.admindb )
            print( "login works")
            m.collection().insert_one( { "hello" : "world" } )
            m.collection().delete_one( { "hello" : "world" } )
            print( "Insert and delete work")
        except pymongo.errors.OperationFailure, e :
            print( "Exception : Operations Failure: %s" % e.details )

        sys.exit( 0 )
    if args.fieldfile:
        try :
            fieldConfig = FieldConfig( args.fieldfile )
        except OSError, e:
            print( "Field file : '%s' cannot be opened : %s" % (args.fieldfile, e  ))
            sys.exit( 1 )
    
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
            
    
if __name__ == '__main__':
    
    sys.argv.pop( 0 ) # remove script file name 
    mainline( sys.argv )