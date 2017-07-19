#!/usr/bin/env python
'''
Created on 19 Feb 2016

Need tests for skip larger than file size.

@author: jdrumgoole
'''
from __future__ import print_function

import pymongo
from datetime import datetime
from collections import OrderedDict
from dateutil.parser import parse

import csv
import os
import argparse
import sys
import logging
import time

from fieldconfig import FieldConfig, FieldConfigException
from mongodb_utils.mongodb import MongoDB

def skipLines( f, skipCount ):
    '''
    >>> f = open( "test_set_small.txt", "r" )
    >>> skipLines( f , 20 )
    20
    '''
    
    lineCount = 0 
    if ( skipCount > 0 ) :
        #print( "Skipping")
        dummy = f.readline() #skicaount may be bigger thant he number of lines i  the fole
        while dummy :
            lineCount = lineCount + 1
            if ( lineCount == skipCount ) :
                break
            dummy = f.readline()
            
    return lineCount 

def makeFieldFilename( path ):
    '''
    >>> makeFieldFilename( "test.txt" ) 
    'test.ff'
    >>> makeFieldFilename( "test" )
    'test.ff'
    '''
    ( x, _ ) = os.path.splitext( os.path.basename( path ))
    return x + ".ff" 

class BatchWriter(object):
     
    def __init__(self, collection, path, fieldConfig, args, orderedWrites=None ):
         
        self._collection = collection
        self._orderedWrites = orderedWrites
        self._fieldConfig = fieldConfig
        self._chunkSize = args.chunksize
        self._args = args
        self._filename = path
        self._totalWritten = 0
        self._path = path
        self._currentLine = 0
        self._progressFilename = os.path.splitext(os.path.basename(path))[0] + ".pilog"
        
        if args.hasheader :
            self._currentLine = self._currentLine + 1 
            
        if args.restart:
            if os.path.exists( self._progressFilename ):
                with open( self._progressFilename, "rU") as pfile:
                    progress = pfile.readline().rstrip()
                    self._currentLine = self._currentLine + int( progress.split( ':')[1] )
                    print( "Retrieving progress from '%s' : %i reccords written" % ( self._progressFilename, self._currentLine ))
            else:
                print( "Can't open progress file ('%s') for input file: '%s'" % ( path, self._progressFilename ))

            
    def updateProgress(self, totalWritten ):
        with open( self._progressFilename, "w") as pfile:
            pfile.write( "progress: %i\n" % totalWritten )
    
    def bulkWrite(self, file_timestamp ):
        '''
        Write the contents of the file to MongoDB potentially adding timestamps and file stamps
        
        self._totalRead = all the lines read from the file including headers.
        self._totalWritten = all the lines written to MongoDB. Not we don't write the header line.
        This becomes important during restarts because in a restart the restart count starts from the non-headerline
        so we must skip the header and then start skipping lines.
        '''
        
        with open( self._path, "rU") as f :
            
            skipLines( f, self._currentLine )
            
 
            if self._orderedWrites :
                bulker = self._collection.initialize_ordered_bulk_op()
            else:
                bulker = self._collection.initialize_unordered_bulk_op()
                
            timeStart = time.time() 
            bulkerCount = 0
            insertedThisQuantum = 0
            totalRead = 0
    
            reader = csv.DictReader( f, fieldnames = self._fieldConfig.fields(), delimiter = self._args.delimiter )

            for dictEntry in reader :
                totalRead = totalRead + 1
                if self._args.addfilename :
                    file_path = self._filename
                else:
                    file_path =  None
                    
                d = self._fieldConfig.createDoc( file_timestamp, file_path,  dictEntry, totalRead )
                bulker.insert( d )
                bulkerCount = bulkerCount + 1 
                if ( bulkerCount == self._chunkSize ):
                    result = bulker.execute()
                    self._totalWritten = self._totalWritten + result[ 'nInserted' ]
                    self.updateProgress( self._totalWritten )
                    insertedThisQuantum = insertedThisQuantum + result[ 'nInserted' ]
                    if self._orderedWrites :
                        bulker = self._collection.initialize_ordered_bulk_op()
                    else:
                        bulker = self._collection.initialize_unordered_bulk_op()
                 
                    bulkerCount = 0
                timeNow = time.time()
                if timeNow > timeStart + 1  :
                    print( "Input: '%s' : records written per second %i, records read: %i written: %i" % ( self._path, insertedThisQuantum, totalRead, self._totalWritten ))
                    insertedThisQuantum = 0
                    timeStart = timeNow
             
            if insertedThisQuantum > 0 :
                print( "Input: '%s' : records written per second %i, records read: %i written: %i" % ( self._path, insertedThisQuantum, totalRead, self._totalWritten  ))

            if ( bulkerCount > 0 ) :
                result = bulker.execute()
                self._totalWritten = self._totalWritten + result[ 'nInserted' ]
                self.updateProgress( self._totalWritten )
                print( "Input: '%s' : Inserted last %i records" % ( self._path, result[ 'nInserted'] ))
 

            print( "Total records read: %i, totalWritten: %i" % ( totalRead, self._totalWritten ))
            return  self._totalWritten

        
# def tracker(totalCount, result, filename ):
#     totalCount = totalCount + result[ "nInserted" ]
#     #print("Inserted %d records from %s" % ( totalCount, filename ))
#     return totalCount 


       
# def printRecord( names, rec, end="\n" ):
#     print ("{",)
#     for name in fieldNames :
#         print ( "   %s = %s, " %  ( name, rec[ name ] ), end=end)
#     print( "}")
#  

# def multiProcessFiles( fieldConfig, args ):
#     
#     importerProcs = []
#     
#     try: 
#         for i in args.filenames :
#             name="reader: %s" % i
#             importerProc = multiprocessing.Process( name=name, target= processOneFile, args=( fieldConfig, args, i, name ))
#             importerProcs.append( importerProc )
#             importerProc.start()
#                 
#         for p in importerProcs :
#             p.join()
#         
#     except KeyboardInterrupt :
#         for p in importerProcs :
#             p.terminate()
#             p.join()

class InputFileException(Exception):
    def __init__(self, *args,**kwargs):
        Exception.__init__(self,*args,**kwargs)

def processOneFile( collection, fieldConfig, file_timestamp, args, path ):
        
    if args.fieldfile is None :
        fieldFilename = makeFieldFilename( path )
        try : 
            fieldConfig= FieldConfig( fieldFilename )
        except OSError, e:
            raise FieldConfigException( "no valid field file for :%s"  % fieldFilename )

    bw = BatchWriter( collection, path, fieldConfig, args )
    totalWritten = bw.bulkWrite( file_timestamp  )

    return ( path, totalWritten )
            
  
def processFiles( collection, fieldConfig, file_timestamp, args ):
    
    totalCount = 0
    lineCount = 0
    results=[]
    failures=[]
    
    for i in args.filenames :
        try:
            print ("Processing : %s" % i )
            ( filename, lineCount ) = processOneFile( collection, fieldConfig, file_timestamp, args, i  )
            totalCount = lineCount + totalCount
            results.append( filename )
        except FieldConfigException, e :
            print( "Field file error for %s : %s" % ( i, e ))
            failures.append( i )
        except InputFileException, e :
            print( "Input file error for %s : %s" % ( i, e ))
            failures.append( i )
            
    if len( results ) > 0 :
        print( "Processed  : %i files" % len( results ))
    if len( failures ) > 0 :
        print( "Failed to process : %s" % failures )
        

def mainline( args ):
    
    __VERSION__ = "1.3"
    
    '''
    
    1.3 : Added lots of support for the NHS Public Data sets project. --addfilename and --addtimestamp.
    Also we now fail back to string when type conversions fail.
    
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
    
    Each file in the input list must correspond to a fieldfile format that is
    common across all the files. The fieldfile is specified by the 
    --fieldfile parameter.
    
    An example run:
    
    python pymongodbimport.py --database demo --collection demo --fieldfile test_set_small.ff test_set_small.txt
    '''
    parser = argparse.ArgumentParser( prog = "pymongodbimport", usage=usage_message )
    parser.add_argument( '--database', default="test", help='specify the database name')
    parser.add_argument( '--collection', default="test", help='specify the collection name')
    parser.add_argument( '--host', default="mongodb://localhost:27017/test", help='mongodb://localhost:270017 : std URI arguments apply')
    parser.add_argument( '--chunksize', type=int, default=500, help='set chunk size for bulk inserts' )
    parser.add_argument( '--restart', default=False, action="store_true", help="use record count insert to restart at last write")
    parser.add_argument( '--insertmany', default=False, action="store_true", help="use insert_many")
    parser.add_argument( '--testlogin', default=False, action="store_true", help="test database login")
    parser.add_argument( '--drop', default=False, action="store_true", help="drop collection before loading" )
    parser.add_argument( '--ordered', default=False, action="store_true", help="forced ordered inserts" )
    parser.add_argument( '--verbose', default=0, type=int, help="controls how noisy the app is" )
    parser.add_argument( "--fieldfile", default= None, type=str,  help="Field and type mappings")
    parser.add_argument( "--delimiter", default=",", type=str, help="The delimiter string used to split fields (default ',')")
    parser.add_argument( 'filenames', nargs="*", help='list of files')
    parser.add_argument('--version', action='version', version='%(prog)s version:' + __VERSION__ )
    parser.add_argument('--addfilename', default=False, action="store_true", help="Add file name field to every entry" )
    parser.add_argument('--addtimestamp', default="none", help="Add a timestamp to each record" )
    parser.add_argument('--hasheader',  default=False, action="store_true", help="Use header line for column names")
    parser.add_argument( '--genfieldfile', default=None, help="Generate a fieldfile from the data file")
    
    args= parser.parse_args( args )

    fieldConfig = None
        
    client = MongoDB( args.host).client()
    database = client[ args.database ]
    collection = database[ args.collection ]
        
    if args.testlogin:
        try: 
            print( "login works")
            collection.insert_one( { "hello" : "world" } )
            collection.delete_one( { "hello" : "world" } )
            print( "Insert and delete work")
        except pymongo.errors.OperationFailure, e :
            print( "Exception : Operations Failure: %s" % e.details )

        sys.exit( 0 )
        
    if args.fieldfile:
        try :
            fieldConfig = FieldConfig( args.fieldfile, args.hasheader )
        except OSError, e:
            print( "Field file : '%s' cannot be opened : %s" % (args.fieldfile, e  ))
            sys.exit( 1 )
    elif args.genfieldfile :
        print( "Generating a field file from '%s'"  % args.genfieldfile )
        genfilename = FieldConfig.generate_field_file( args.genfieldfile, args.delimiter, ext=".ff" )
        print("Generated: '%s'" % genfilename )

    if args.drop :
        if args.restart :
            print( "Warning --restart overrides --drop ignoring drop commmand")
        else:
            database.drop_collection( args.collection )
            print( "dropped collection: %s.%s" % ( args.database, args.collection ))
    
    file_timestamp = None
    
    if args.addtimestamp :
        if args.addtimestamp == "generate" :
            file_timestamp  = args.addtimestamp
        elif args.addtimestamp == "none" :
            file_timestamp = None
        elif args.addtimestamp == "now" :
            file_timestamp = datetime.utcnow()
        else:
            file_timestamp  = parse( args.addtimestamp )
         
    if args.filenames:   
        print(  "Using database: %s, collection: %s" % ( args.database, args.collection ))
        print( "processing %i files" % len( args.filenames ))
    
        if args.chunksize < 1 :
            print( "Chunksize must be 1 or more. Chunksize : %i" % args.chunksize )
            sys.exit( 1 )
        
        processFiles( collection, fieldConfig,file_timestamp, args )
            
    
if __name__ == '__main__':
    
    sys.argv.pop( 0 ) # remove script file name 
    mainline( sys.argv )
