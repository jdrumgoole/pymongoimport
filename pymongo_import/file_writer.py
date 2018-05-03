'''
Created on 23 Jul 2017

@author: jdrumgoole


'''
#import os
import time
#import pprint
import logging

from pymongo_import.restart import Restarter
from pymongo_import.logger import Logger
from datetime import datetime, timedelta

from collections import OrderedDict

def seconds_to_duration( seconds ):
    delta = timedelta( seconds=seconds )
    d = datetime(1,1,1) + delta
    return "%02d:%02d:%02d:%02d" % (d.day-1, d.hour, d.minute, d.second)

class File_Writer(object):

     
    def __init__(self, collection, fieldConfig, encoding="utf-8", batch_size=500, orderedWrites=None ):
         
        self._logger = logging.getLogger( Logger.LOGGER_NAME )
        self._collection = collection
        self._orderedWrites = orderedWrites
        self._fieldConfig = None
        self._batch_size = batch_size
        self._totalWritten = 0
        self._encoding = encoding
        self._currentLine = 0
        self._fieldConfig = fieldConfig
        if fieldConfig.hasheader() :
            self._currentLine = self._currentLine + 1 
            
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

    def has_locator(self, collection, filename):

        result = collection.find_one({ "locator" : { "f": filename}})
        return result

    def add_locator(self, collection, doc, filename, record_number):

        if filename and record_number:
            doc['locator'] = { "f":filename, "n":record_number}
        elif filename:
            doc['locator'] = {"f": filename}
        elif record_number:
            doc['locator'] = {"n": record_number}

        return doc

    def insert_file(self, filename, restart=False ):
        
        start = time.time()
        total_written = 0
        results = None

        #with open( filename, "r", encoding = "ISO-8859-1") as f :

        with open(filename, "r" ) as f:
            timeStart = time.time() 
            insertedThisQuantum = 0
            total_read = 0
            insert_list = []
            restarter = None 
            
            
            if restart :
                restarter = Restarter( self._collection.database,  filename, self._batch_size )
                skip_count = restarter.restart( self._collection  )
                if skip_count > 0 :
                    self._logger.info( "Restarting : skipping %i lines", skip_count )
                    self._currentLine = self._currentLine + skip_count
                    
            self.skipLines(f, self._currentLine ) # skips header if present
                  
            reader = self._fieldConfig.get_dict_reader( f )

            try:
                for dictEntry in reader :
                    total_read = total_read + 1
                    if len( dictEntry ) == 1 :
                        self._logger.warning( "Warning: only one field in input line. Do you have the right delimiter set ? ( current delimiter is : '%s')", self._fieldConfig.delimiter())
                        self._logger.warning( "input line : '%s'", "".join( dictEntry.values()))


                    d = self._fieldConfig.createDoc(OrderedDict(), dictEntry)

                    d = self.add_locator( self._collection, d, filename, total_read)

                    insert_list.append( d )
                    if total_read % self._batch_size == 0 :
                        results = self._collection.insert_many( insert_list )
                        total_written = total_written + len( results.inserted_ids )
                        insertedThisQuantum = insertedThisQuantum + len( results.inserted_ids )
                        if restarter :
                            restarter.update( results.inserted_ids[ -1], total_written )
                        insert_list = []
                        time_now = time.time()
                        elapsed = time_now - timeStart
                        docs_per_second = self._batch_size/elapsed
                        timeStart = time_now
                        self._logger.info( "Input:'{}': docs per sec:{:7.0f}, total docs:{:>10}".format( filename, docs_per_second, total_written ))

            except UnicodeDecodeError as exp:
                self._logger.error(exp)
                self._logger.error( "Error on line:%i", total_read + 1 )
                raise;
                        
            if len( insert_list ) > 0  :
                results = self._collection.insert_many( insert_list )
                total_written = total_written + len( results.inserted_ids )
                if restarter :
                    restarter.update( results.inserted_ids[ -1 ], total_written )
                insert_list = []
                self._logger.info( "Input: '%s' : Inserted %i records", filename, total_written )
                
        finish = time.time()
        if restarter :
            restarter.finish()
        self._logger.info( "Total elapsed time to upload '%s' : %s" , filename,seconds_to_duration( finish - start ))


        return total_written
    
#     def bulkWrite(self, filename  ):
#         '''
#         Write the contents of the file to MongoDB potentially adding timestamps and file stamps
#         
#         self._totalRead = all the lines read from the file including headers.
#         self._totalWritten = all the lines written to MongoDB. We can't restart bulkWrites because
#         we don't know the id of the last object written.
#         '''
#         with open( filename, "r") as f :
#             
#             BulkWriter.skipLines( f, self._currentLine )
#  
#             if self._orderedWrites :
#                 bulker = self._collection.initialize_ordered_bulk_op()
#             else:
#                 bulker = self._collection.initialize_unordered_bulk_op()
#                 
#             timeStart = time.time() 
#             bulkerCount = 0
#             insertedThisQuantum = 0
#             totalRead = 0
#     
#             reader = self._fieldConfig.get_dict_reader( f )
# 
#             for dictEntry in reader :
#                 totalRead = totalRead + 1
#                     
#                 d = self._fieldConfig.createDoc( dictEntry )
#                 bulker.insert( d )
#                 bulkerCount = bulkerCount + 1 
#                 if ( bulkerCount == self._batch_size ):
#                     result = bulker.execute()
#                     bulkerCount = 0
#                     self._totalWritten = self._totalWritten + result[ 'nInserted' ]
#                     insertedThisQuantum = insertedThisQuantum + result[ 'nInserted' ]
#                     if self._orderedWrites :
#                         bulker = self._collection.initialize_ordered_bulk_op()
#                     else:
#                         bulker = self._collection.initialize_unordered_bulk_op()
#                  
# 
#                 timeNow = time.time()
#                 if timeNow > timeStart + 1  :
#                     self._logger.info( "Input: '%s' : records written per second %i, records read: %i written: %i", filename, insertedThisQuantum, totalRead, self._totalWritten )
#                     insertedThisQuantum = 0
#                     timeStart = timeNow
#              
#             if insertedThisQuantum > 0 :
#                 self._logger.info( "Input: '%s' : records written per second %i, records read: %i written: %i", filename, insertedThisQuantum, totalRead, self._totalWritten )
# 
#             if ( bulkerCount > 0 ) :
#                 result = bulker.execute()
#                 self._totalWritten = self._totalWritten + result[ 'nInserted' ]
#                 self._logger.info( "Input: '%s' : Inserted last %i records", filename, result[ 'nInserted'] )
#  
# 
#             self._logger.info( "Total records read: %i, totalWritten: %i", totalRead, self._totalWritten )
#             return  self._totalWritten

