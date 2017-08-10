'''
Created on 24 Jul 2017

@author: jdrumgoole
'''

from pymongodbimport.fieldconfig import FieldConfigException, FieldConfig
from pymongodbimport.bulkwriter import BulkWriter
import os

class InputFileException(Exception):
    def __init__(self, *args,**kwargs):
        Exception.__init__(self,*args,**kwargs)
        
class FileProcessor( object ):
    
    def __init__(self, collection, delimiter, onerror="warn", gen_id="mongodb", batchsize=500):
        self._collection = collection
        self._delimiter = delimiter
        self._onerror = onerror
        self._gen_id = gen_id
        self._batchsize = batchsize 
        
    def processOneFile( self, field_filename, input_filename, hasheader, restart ):
            
        fieldConfig = FieldConfig( field_filename, self._delimiter, hasheader, self._gen_id, self._onerror )
    
        bw = BulkWriter( self._collection, fieldConfig, self._batchsize )
        totalWritten = bw.insert_file( input_filename, restart )
        return totalWritten 
    
    def processFiles( self, filenames, hasheader, field_filename, restart ):
        
        totalCount = 0
        lineCount = 0
        results=[]
        failures=[]
        new_name = None
        
        for i in filenames :
            
            try:
                print ("Processing : %s" % i )
                if field_filename :
                    new_name = field_filename
                    print( "using field file: '%s'" % field_filename )
                elif hasheader :
                    new_name = FieldConfig.generate_field_file( i )
                    print( "Created field file: '%s'" % field_filename )
                else:
                    new_name = os.path.splitext(os.path.basename( i ))[0] + ".ff" 
                   
                lineCount = self.processOneFile( new_name, i, hasheader, restart )
                totalCount = lineCount + totalCount
            except FieldConfigException, e :
                print( "FieldConfig error for %s : %s" % ( i, e ))
                failures.append( i )
                if self._onerror == "fail":
                    raise
            except InputFileException, e :
                print( "Input file error for %s : %s" % ( i, e ))
                failures.append( i )
                if self._onerror == "fail":
                    raise
                
        if len( results ) > 0 :
            print( "Processed  : %i files" % len( results ))
        if len( failures ) > 0 :
            print( "Failed to process : %s" % failures )
