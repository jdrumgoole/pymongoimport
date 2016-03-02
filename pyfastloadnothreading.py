'''
Created on 19 Feb 2016

@author: jdrumgoole
'''
from __future__ import print_function

import pymongo
from datetime import datetime
from collections import OrderedDict

import csv
import os
import argparse
import string
from pprint import pprint

def getFields( filename ):
    
    fieldDict = OrderedDict()
    with open( filename, "r" ) as f :
        l = f.readline().strip()
        while l :
            if l.startswith('#' ) :
                continue
            values = string.split( l, ":", maxsplit=2)
            if len( values ) > 2 :
                fieldDict[ values[0].strip() ] = { "type" : values[ 1 ].strip(), "format" : values[ 2 ].strip() }
            else:
                fieldDict[ values[0].strip() ] = { "type" : values[ 1 ].strip()  }
            l = f.readline().strip()
    return fieldDict 
     
def createDoc( fieldDict, line ):

    doc = OrderedDict()
    for k in fieldDict :
        #print( "line: %s" % line )
        if fieldDict[ k ]["type" ] == "_id" :
            doc[ "_id" ] = int( line[ k ] )
        elif fieldDict[ k ]["type" ] == "str" :
            doc[ k ] = line[ k ]
        elif fieldDict[ k ]["type"] == "int" :
            doc[ k ] = int( line[ k ])
        elif fieldDict[ k ]["type" ] == "date" :
            if line[ k ] == "NULL" :
                doc[ k ] = "NULL"
            else:
                doc[ k ] = datetime.strptime( line[ k ], fieldDict[ k ][ "format"] )
    
    return doc
        
def tracker(totalCount, result, filename ):
    totalCount = totalCount + result[ "nInserted" ]
    #print("Inserted %d records from %s" % ( totalCount, filename ))
    return totalCount 

def writeChunk( collection, reader, fieldDict, chunkSize, ordered ):
    bulker = None
    try : 
        if ordered :
            bulker = collection.initialize_ordered_bulk_op()
        else:
            bulker = collection.initialize_unordered_bulk_op()
            
        count = 0
        for r in reader :
            d = createDoc( fieldDict, r )
            #r["TestDate"] = datetime.strptime( r["TestDate"],"%Y-%m-%d")
            #print( "Line")
            #pprint( r )
            #print( "Doc" )
            #pprint( d ) 
            bulker.insert( d )
            count = count + 1 
            if ( count == chunkSize ):
                break
        
        if ( count > 0 ) :
            return bulker.execute()
        else:
            return { "nInserted" : 0 }
    except pymongo.errors.BulkWriteError as e :
        print( "Bulk write error : %s" % e )
        raise
       
def printRecord( names, rec, end="\n" ):
    print ("{",)
    for name in fieldNames :
        print ( "   %s = %s, " %  ( name, rec[ name ] ), end=end)
    print( "}")
 
if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description='loader for MOT data', prog = "pyfastloader")
    parser.add_argument('--database', default="test", help='specify the database name')
    parser.add_argument('--collection', default="dataload", help='specify the collection name')
    parser.add_argument('--host', default="localhost", help='hostname')
    parser.add_argument('--port', default="27017", help='port name', type=int)
    parser.add_argument('--chunksize', default="5000", help='set chunk size for bulk inserts', type=int)
    parser.add_argument('filenames', nargs="+", help='list of files')
    parser.add_argument( '--skip', default=0, type=int, help="skip lines before reading")
    parser.add_argument( '--drop', default=False, action="store_true", help="drop collection before loading" )
    parser.add_argument( '--ordered', default=False, action="store_true", help="forced ordered inserts" )
    parser.add_argument( '--verbose', default=0, type=int, help="controls how noisy the app is" )
    parser.add_argument( "--fieldfile", default="fieldfile", type=str,  help="Field and type mappings")
    args= parser.parse_args()
    
    mc = pymongo.MongoClient( host=args.host, port=args.port )
    db = mc[ args.database ]
    filenames = args.filenames
    collection = db[ args.collection ]
    
    if args.drop :
        collection.drop()
        print( "dropped collection: %s.%s" % ( args.database, args.collection ))
    
    print(  "database : %s, collection %s" % ( args.database, args.collection ))
    print( "files %s" % args.filenames )
    fieldNames = [ "TestID", "VehicleID", "TestDate", "TestClassID", 
                  "TestType", "TestResult", "Test Mileage", "Postcode", 
                  "Make", "Model", "Colour","FuelType", "CylinderCapacity", "FirstUseDate" ]
    

    fieldDict = None
    if os.path.isfile( args.fieldfile ):
        fieldDict = getFields( args.fieldfile )
        
    #print( "FieldDict : %s " % fieldDict )
    totalCount = 0
    if ( args.chunksize > 0 ) :
        bulkerSize = args.chunksize 
    else:
        bulkerSize = 5000
    
    for i in filenames:
        print ("Processing : %s" % i )
        if ( os.path.exists( i ) and os.path.isfile( i )) :
            with open( i, "r") as f :
                if ( args.skip > 0 ) :
                    print( "Skipping")
                    count = 0 
                    dummy = f.readline()
                    while dummy :
                        count = count + 1
                        if ( count == args.skip ) :
                            break ;
                        f.readline()
                #print( "arg.skips %i" % args.skip )
                reader = csv.DictReader( f, fieldnames = fieldDict.keys(), delimiter = '|')
                
                while True :
                    result = writeChunk( collection, reader, fieldDict, bulkerSize, args.ordered )
                    totalCount = tracker( totalCount, result, i )
                    if result[ "nInserted" ] < bulkerSize :
                        break
                    
                print( "Completed processing : %s, (%d records)" % ( i, totalCount ))
            
    for i in filenames :
        print( "Processed %s" % i )
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
        