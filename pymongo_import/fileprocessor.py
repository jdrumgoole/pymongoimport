'''
Created on 24 Jul 2017

@author: jdrumgoole
'''

import os
import logging

from pymongo_import.fieldconfig import FieldConfigException, FieldConfig
from pymongo_import.file_writer import File_Writer
from pymongo_import.logger import Logger
from datetime import datetime

class InputFileException(Exception):
    def __init__(self, *args,**kwargs):
        Exception.__init__(self,*args,**kwargs)
        
class FileProcessor( object ):
    
    def __init__(self, collection, delimiter, onerror="warn", gen_id="mongodb", batchsize=500):
        self._logger = logging.getLogger( Logger.LOGGER_NAME  )
        self._collection = collection
        self._delimiter = delimiter
        self._onerror = onerror
        self._gen_id = gen_id
        self._batchsize = batchsize
        self._files = []
        
    def processOneFile( self, input_filename, field_filename=None, hasheader=False, restart=False,batchID=None ):
            
        if not field_filename:
            field_filename = FieldConfig.generate_field_filename(input_filename)

        if not os.path.isfile(field_filename):
            self._logger.error( "The fieldfile '{}' does not exit".format( field_filename))
            raise ValueError( "No such field file: '{}".format(field_filename))

        self._logger.info("using field file: '%s'", field_filename)
        fieldConfig = FieldConfig( field_filename, self._delimiter, hasheader, self._gen_id, self._onerror )
    
        fw = File_Writer( self._collection, fieldConfig, self._batchsize )
        totalWritten = fw.insert_file( input_filename, restart)
        return totalWritten 

    def get_files(self):
        return self._files

    def processFiles( self, filenames, field_filename=None, hasheader=False, restart=False, audit=None, batchID=None ):
        
        totalCount = 0
        lineCount = 0
        results=[]
        failures=[]
        new_name = None
        
        for i in filenames :
            self._files.append(i)
            try:
                self._logger.info("Processing : %s", i )
#                 if field_filename :
#                     new_name = field_filename
#                     self._logger.info( "using field file: '%s'", new_name )
#                 else:
#                     new_name = os.path.splitext(os.path.basename( i ))[0] + ".ff" 
                lineCount = self.processOneFile( i, field_filename, hasheader, restart )
                size = os.path.getsize( i )
                path = os.path.abspath( i )
                if audit and batchID:
                    audit.add_batch_info( batchID,"file_data", { "size"       : size,
                                                                 "collection" : self._collection.full_name,
                                                                 "path"       : path,
                                                                 "records"    : lineCount,
                                                                 "timestamp"  : datetime.utcnow()})

                totalCount = lineCount + totalCount
            except FieldConfigException as e :
                self._logger.info( "FieldConfig error for %s : %s", i, e )
                failures.append( i )
                if self._onerror == "fail":
                    raise
            except InputFileException as e :
                self._logger.info( "Input file error for %s : %s", i, e )
                failures.append( i )
                if self._onerror == "fail":
                    raise
                
        if len( results ) > 0 :
            self._logger.info( "Processed  : %i files", len( results ))
        if len( failures ) > 0 :
            self._logger.info( "Failed to process : %s", failures )
