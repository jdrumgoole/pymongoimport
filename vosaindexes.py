'''
Created on 10 Jun 2016

@author: jdrumgoole
'''
import pymongo
import argparse

if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description='loader for MOT data', prog = "vosaindexes")
    parser.add_argument('--database', default="vosa", help='specify the database name')
    parser.add_argument('--collection', default="results2013", help='specify the collection name')
    parser.add_argument('--host', default="localhost", help='hostname')
    parser.add_argument('--port', default="27017", help='port name', type=int)
    parser.add_argument('--username', default=None, help='username to login to database')
    parser.add_argument('--password', default=None, help='password to login to database')
    parser.add_argument('--background', default=False, action="store_true",  help='build indexes in background')
    parser.add_argument('--ssl', default=False, action="store_true", help='use SSL for connections')

    args= parser.parse_args()
    
    client = pymongo.MongoClient( host=args.host, port=args.port, ssl=args.ssl)
    database = client[ args.database ]
    
    if args.username :
        if database.authenticate( args.username, args.password ):
#            if self._database.authenticate( self._username, self._password, mechanism='MONGODB-CR'):
            print( "successful login by %s" % args.username )
        else:
            print( "login failed for %s" % args.username )
            
    collection = database[ args.collection ]
    indexes = [ "TestID", "VehicleID", "TestClassID", "Make", "Model", "Colour", "TestMileage", "TestDate", "FirstUseDate"]
   
    print( "creating : %s" % indexes )
    for i in indexes :
        print( "Creating index: '%s' (background: %s)..." % ( i, args.background ))
        index = collection.create_index([(i, pymongo.ASCENDING)], background = args.background )
        print( "Created index named: '%s'" % index )
        
    print("Created all indexes")
