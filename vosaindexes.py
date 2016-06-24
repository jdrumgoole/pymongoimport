'''
Created on 10 Jun 2016

@author: jdrumgoole
'''
from mongodb import MongoDB
import argparse
import pymongo

if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description='loader for MOT data', prog = "vosaindexes")
    parser.add_argument('--database', default="vosa", help='specify the database name')
    parser.add_argument('--collection', default="results_2013", help='specify the collection name')
    parser.add_argument('--host', default="localhost", help='hostname')
    parser.add_argument('--port', default="27017", help='port name', type=int)
    parser.add_argument( '--admindb', default="admin", help="Admin database used for authentication" )
    parser.add_argument('--username', default=None, help='username to login to database')
    parser.add_argument('--password', default=None, help='password to login to database')
    parser.add_argument('--background', default=False, action="store_true",  help='build indexes in background')
    parser.add_argument('--ssl', default=False, action="store_true", help='use SSL for connections')

    args= parser.parse_args()
    
    m = MongoDB( args.host, args.port, args.database, args.collection, args.username, args.password, ssl=args.ssl, admindb=args.admindb )
    m.connect(source=args.admindb )
    
    indexes = [ "TestID", "VehicleID", "TestClassID", "Make", "Model", "Colour", "TestMileage", "TestDate", "FirstUseDate"]
   
    print( "creating : %s" % indexes )
    for i in indexes :
        print( "Creating index: '%s' (background: %s)..." % ( i, args.background ))
        index = m.collection().create_index([(i, pymongo.ASCENDING)], background = args.background )
        print( "Created index named: '%s'" % index )
        
    print("Created all indexes")
