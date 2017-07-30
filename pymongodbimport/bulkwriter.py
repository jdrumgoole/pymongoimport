'''
Created on 23 Jul 2017

@author: jdrumgoole
'''
import os
import time
import csv
import tempfile
from pymongodbimport.fieldconfig import FieldConfig, FieldConfigException

class BulkWriter(object):
     
    def __init__(self, collection, input_filename, args, orderedWrites=None ):
         
        self._collection = collection
        self._orderedWrites = orderedWrites
        self._fieldConfig = None
        self._chunkSize = args.chunksize
        self._args = args
        self._filename = input_filename
        self._totalWritten = 0
        self._restartFilename = os.path.splitext(os.path.basename(input_filename ))[0] + ".restart.log"
        self._currentLine = 0
        self._restartFile = None
        
        if args.genfieldfile :  # generate a file
            print( "Generating a field file from '%s'"  % input_filename  )
            field_filename = FieldConfig.generate_field_file( input_filename, args.delimiter, ext=".ff" )
            print("Generated: '%s'" % field_filename )
            hasheader = True
        elif args.fieldfile: # use the file on the command line
            field_filename = args.fieldfile
            hasheader = args.hasheader
        else:
            field_filename = FieldConfig.generate_field_filename( input_filename ) # use an existing fle
            hasheader = True
    
            try :
                self._fieldConfig = FieldConfig( field_filename,
                                                  input_filename,
                                                  hasheader, 
                                                  args.addfilename, 
                                                  args.addtimestamp, 
                                                  args.id)
            except FieldConfigException :
                raise
            
        if hasheader :
            self._currentLine = self._currentLine + 1 
            
        if args.restart:
            self._currentLine = self.restart( self._currentLine)
            
    def restart(self, currentLine ):
        if os.path.exists( self._restartFilename ):
            with open( self._restartFilename, "rU") as pfile:

                currentID = pfile.readline().rstrip()
                progress = pfile.readline().rstrip()
                currentLine = self._currentLine + int( progress.split( ':')[1] )
                print( "Retrieving restart point from '%s' : %i records written" % ( self._restartFilename, currentLine ))
        else:
            print( "Can't open restart file ('%s') for input file: '%s'" % ( self._filename, self._restartFilename ))
        
        return currentLine
        
    def update_restart_log(self, collection, totalWritten ):
        
        name = None 
        with tempfile.NamedTemporaryFile( mode="w", dir=os.getcwd(), delete=False ) as tempFile :
            doc = collection.find().sort({"$natural" :-1}).limit( 1 )
            self._restartFile.write( "id : %s\nprogress: %i\n" % doc[ "_id" ], totalWritten )
            self._restartFile.flush()
            name = tempFile.name()
            
        os.rename( name, self._restartFile )
        
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
                    self.update_restart_log( self._totalWritten )
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
)               self.update_restart_log( self._totalWritten )
                print( "Input: '%s' : Inserted last %i records" % ( self._filename, result[ 'nInserted'] ))
 

            print( "Total records read: %i, totalWritten: %i" % ( totalRead, self._totalWritten ))
            return  self._totalWritten

