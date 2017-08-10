'''
Created on 23 Jul 2017

@author: jdrumgoole
'''
import os
import time
import pprint
from pymongodbimport.restart import Restarter

class BulkWriter(object):
     
    def __init__(self, collection, fieldConfig, batchsize=500, restart=False, orderedWrites=None ):
         
        self._collection = collection
        self._orderedWrites = orderedWrites
        self._fieldConfig = None
        self._batchsize = batchsize
        self._totalWritten = 0
        self._restart = restart

        self._currentLine = 0
        self._restartFile = None
        self._fieldConfig = fieldConfig
        if fieldConfig.hasheader() :
            self._currentLine = self._currentLine + 1 
            
        self._restarter = None
        if self._restart:
            self._restarter = Restarter( self._collection.database, "dummy", self._batchsize )
            self._currentLine  = self._currentLine + self._restarter.restart( self._collection )
            
    @staticmethod
    def skipLines( f, skipCount ):
        '''
        >>> f = open( "test_set_small.txt", "r" )
        >>> skipLines( f , 20 )
        20
        '''
        
        lineCount = 0 
        if ( skipCount > 0 ) :
            #print( "Skipping")
            dummy = f.readline() #skicaount may be bigger than the number of lines i  the file
            while dummy :
                lineCount = lineCount + 1
                if ( lineCount == skipCount ) :
                    break
                dummy = f.readline()
                
        return lineCount 

    def insert_file(self, filename, restart = None ):
        
        start = time.time()
        total_written = 0

        with open( filename, "rU") as f :
            
            timeStart = time.time() 
            insertedThisQuantum = 0
            total_read = 0
            insert_list = []
            restarter = None 
            
            
            if restart :
                restarter = Restarter( self._collection.database,  filename, self._batchsize )
                skip_count = restarter.restart( self._collection  )
                if skip_count > 0 :
                    print( "Restarting : skipping %i lines" % skip_count )
                    self._currentLine = self._currentLine + skip_count
                    
            self.skipLines(f, self._currentLine ) # skips header if present
                  
            reader = self._fieldConfig.get_dict_reader( f )

            for dictEntry in reader :
                total_read = total_read + 1 
                if len( dictEntry ) == 1 :
                    print( "Warning: only one field in input line. Do you have the right delimiter set ? ( current delimiter is : '%s')" % self._fieldConfig.delimiter())
                    print( "input line : '%s'" % "".join( dictEntry.values()))

                d = self._fieldConfig.createDoc( dictEntry )
                insert_list.append( d )
                if total_read % self._batchsize == 0 :
                    results = self._collection.insert_many( insert_list )
                    total_written = total_written + len( results.inserted_ids )
                    if restarter :
                        restarter.update( results.inserted_ids[ -1], total_written )
                    insert_list = []
                    timeNow = time.time()
                    if timeNow > timeStart + 1  :
                        print( "Input: '%s' : records written per second %i, records read: %i written: %i" % ( filename, insertedThisQuantum, total_read, total_written ))
                        insertedThisQuantum = 0
                        timeStart = timeNow
                        
            if len( insert_list ) > 0  :
                results = self._collection.insert_many( insert_list )
                total_written = total_written + len( results.inserted_ids )
                if restarter :
                    restarter.update( results.inserted_ids[ -1 ], total_written )
                insert_list = []
                print( "Input: '%s' : Inserted %i records" % ( filename, total_written ))
                
        finish = time.time()
        print( "Total elapsed time to upload '%s' : %.3f" %  ( filename,finish - start ))
        if restarter :
            restarter.reset()
        return total_written
    
    def bulkWrite(self, filename  ):
        '''
        Write the contents of the file to MongoDB potentially adding timestamps and file stamps
        
        self._totalRead = all the lines read from the file including headers.
        self._totalWritten = all the lines written to MongoDB. We can't restart bulkWrites because
        we don't know the id of the last object written.
        '''
        with open( filename, "rU") as f :
            
            BulkWriter.skipLines( f, self._currentLine )
 
            if self._orderedWrites :
                bulker = self._collection.initialize_ordered_bulk_op()
            else:
                bulker = self._collection.initialize_unordered_bulk_op()
                
            timeStart = time.time() 
            bulkerCount = 0
            insertedThisQuantum = 0
            totalRead = 0
    
            reader = self._fieldConfig.get_dict_reader( f )

            for dictEntry in reader :
                totalRead = totalRead + 1
                    
                d = self._fieldConfig.createDoc( dictEntry )
                bulker.insert( d )
                bulkerCount = bulkerCount + 1 
                if ( bulkerCount == self._batchsize ):
                    result = bulker.execute()
                    bulkerCount = 0
                    self._totalWritten = self._totalWritten + result[ 'nInserted' ]
                    insertedThisQuantum = insertedThisQuantum + result[ 'nInserted' ]
                    if self._orderedWrites :
                        bulker = self._collection.initialize_ordered_bulk_op()
                    else:
                        bulker = self._collection.initialize_unordered_bulk_op()
                 

                timeNow = time.time()
                if timeNow > timeStart + 1  :
                    print( "Input: '%s' : records written per second %i, records read: %i written: %i" % ( filename, insertedThisQuantum, totalRead, self._totalWritten ))
                    insertedThisQuantum = 0
                    timeStart = timeNow
             
            if insertedThisQuantum > 0 :
                print( "Input: '%s' : records written per second %i, records read: %i written: %i" % ( filename, insertedThisQuantum, totalRead, self._totalWritten  ))

            if ( bulkerCount > 0 ) :
                result = bulker.execute()
                self._totalWritten = self._totalWritten + result[ 'nInserted' ]
                print( "Input: '%s' : Inserted last %i records" % ( filename, result[ 'nInserted'] ))
 

            print( "Total records read: %i, totalWritten: %i" % ( totalRead, self._totalWritten ))
            return  self._totalWritten

