'''
Created on 23 Jul 2017

@author: jdrumgoole
'''
import os
import time
from pymongodbimport.restart import Restarter
class BulkWriter(object):
     
    def __init__(self, collection, fieldConfig, hasheader, chunksize=500, restart=False, orderedWrites=None ):
         
        self._collection = collection
        self._orderedWrites = orderedWrites
        self._fieldConfig = None
        self._chunkSize = chunksize
        self._totalWritten = 0
        self._restart = restart

        self._currentLine = 0
        self._restartFile = None
        self._fieldConfig = fieldConfig
        self._restartFilename = os.path.splitext(os.path.basename( self._fieldConfig.input_filename()))[0] + ".restart.log"
        if hasheader :
            self._currentLine = self._currentLine + 1 
            
        self._restarter = None
        if self._restart:
            self._restarter = Restarter( self._collection.database, self._fieldConfig.input_filename(), self._chunkSize )
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

    def bulkWrite(self ):
        '''
        Write the contents of the file to MongoDB potentially adding timestamps and file stamps
        
        self._totalRead = all the lines read from the file including headers.
        self._totalWritten = all the lines written to MongoDB. Not we don't write the header line.
        This becomes important during restarts because in a restart the restart count starts from the non-headerline
        so we must skip the header and then start skipping lines.
        '''
        filename = self._fieldConfig.input_filename()
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
                if ( bulkerCount == self._chunkSize ):
                    result = bulker.execute()
                    self._totalWritten = self._totalWritten + result[ 'nInserted' ]
                    if self._restart:
                        self._restarter.update( self._totalWritten )
                    insertedThisQuantum = insertedThisQuantum + result[ 'nInserted' ]
                    if self._orderedWrites :
                        bulker = self._collection.initialize_ordered_bulk_op()
                    else:
                        bulker = self._collection.initialize_unordered_bulk_op()
                 
                    bulkerCount = 0
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
                if self._restart:
                    self.update_restart_log( self._totalWritten )
                print( "Input: '%s' : Inserted last %i records" % ( filename, result[ 'nInserted'] ))
 

            print( "Total records read: %i, totalWritten: %i" % ( totalRead, self._totalWritten ))
            return  self._totalWritten

