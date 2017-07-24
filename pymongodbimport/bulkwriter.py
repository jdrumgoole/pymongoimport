'''
Created on 23 Jul 2017

@author: jdrumgoole
'''
import os
import time
import csv

class BulkWriter(object):
     
    def __init__(self, collection, input_filename, fieldConfig, hasheader, args, orderedWrites=None ):
         
        self._collection = collection
        self._orderedWrites = orderedWrites
        self._fieldConfig = fieldConfig
        self._chunkSize = args.chunksize
        self._args = args
        self._filename = input_filename
        self._totalWritten = 0
        self._progressFilename = os.path.splitext(os.path.basename(input_filename ))[0] + ".pilog"
        self._currentLine = 0
        
        if hasheader :
            self._currentLine = self._currentLine + 1 
            
        if args.restart:
            self._currentLine = self.restart( self._currentLine)
            
    def restart(self, currentLine ):
        progressFilename = os.path.splitext(os.path.basename(self._filename ))[0] + ".pilog"
        if os.path.exists( progressFilename ):
            with open( progressFilename, "rU") as pfile:
                progress = pfile.readline().rstrip()
                currentLine = self._currentLine + int( progress.split( ':')[1] )
                print( "Retrieving progress from '%s' : %i records written" % ( progressFilename, currentLine ))
        else:
            print( "Can't open progress file ('%s') for input file: '%s'" % ( self._filename, progressFilename ))
        
        return currentLine
        
    def updateProgress(self, totalWritten ):
        with open( self._progressFilename, "w") as pfile:
            pfile.write( "progress: %i\n" % totalWritten )
    
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
            dummy = f.readline() #skicaount may be bigger thant he number of lines i  the fole
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
        
        with open( self._filename, "rU") as f :
            
            BulkWriter.skipLines( f, self._currentLine )
 
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
                    
                d = self._fieldConfig.createDoc( dictEntry )
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
                    print( "Input: '%s' : records written per second %i, records read: %i written: %i" % ( self._filename, insertedThisQuantum, totalRead, self._totalWritten ))
                    insertedThisQuantum = 0
                    timeStart = timeNow
             
            if insertedThisQuantum > 0 :
                print( "Input: '%s' : records written per second %i, records read: %i written: %i" % ( self._filename, insertedThisQuantum, totalRead, self._totalWritten  ))

            if ( bulkerCount > 0 ) :
                result = bulker.execute()
                self._totalWritten = self._totalWritten + result[ 'nInserted' ]
                self.updateProgress( self._totalWritten )
                print( "Input: '%s' : Inserted last %i records" % ( self._filename, result[ 'nInserted'] ))
 

            print( "Total records read: %i, totalWritten: %i" % ( totalRead, self._totalWritten ))
            return  self._totalWritten

