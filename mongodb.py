'''
Created on 22 Jun 2016

@author: jdrumgoole
'''

import pymongo
import logging

class MongoDB( object ):
    
    def __init__(self, host, port, databaseName, collectionName, 
                 username=None, password=None, ssl=False, admindb="admin"):
        self._host = host
        self._port = port
        self._databaseName = databaseName
        self._collectionName = collectionName
        self._database = None
        self._collection = None
        self._username = username
        self._password = password
        self._ssl = ssl
        self._admindb = admindb
        self._logger = logging.getLogger( databaseName )
    
    def connect(self, source=None):
        
        admindb = self._admindb
        if source:
            admindb = source
            
        self._client = pymongo.MongoClient( host=self._host, port=self._port, ssl=self._ssl)
        self._database = self._client[ self._databaseName]
        
        if self._username :
            if self._database.authenticate( name=self._username, password=self._password, source=admindb):
#            if self._database.authenticate( self._username, self._password, mechanism='MONGODB-CR'):
                logging.debug( "successful login by %s (method SCRAM-SHA-1)", self._username )
            else:
                logging.error( "login failed for %s (method SCRAM-SHA-1)", self._username )
                
        self._collection = self._database[ self._collectionName ]
        
    def collection(self):
        return self._collection

    def database(self):
        return self._database
    