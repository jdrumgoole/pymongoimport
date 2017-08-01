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
    
    def __init__(self, collection ):
        self._collection = collection
        
    def processOneFile( self, field_filename, input_filename, delimiter, hasheader, gen_id, onerror ):
            
        fieldConfig = FieldConfig( field_filename, input_filename, delimiter, gen_id, onerror )
    
        bw = BulkWriter( self._collection, fieldConfig, hasheader )
        totalWritten = bw.bulkWrite()
        return totalWritten 
    
    def processFiles( self, args ):
        
        totalCount = 0
        lineCount = 0
        results=[]
        failures=[]

        
        for i in args.filenames :
            field_filename = os.path.splitext(os.path.basename( i ))[0] + ".ff"
            try:
                print ("Processing : %s" % i )
                if args.genfieldfile :
                    field_filename = FieldConfig.generate_field_file( i )
                    print( "Created field file: '%s'" % field_filename )
                elif args.fieldfile :
                    field_filename = args.fieldfile
                    print( "using field file: '%s'" % field_filename )
                    
                lineCount = self.processOneFile( field_filename, i, args.delimiter, args.hasheader, args.id, args.onerror )
                totalCount = lineCount + totalCount
            except FieldConfigException, e :
                print( "Field file error for %s : %s" % ( i, e ))
                failures.append( i )
                if args.onerror == "fail":
                    raise
            except InputFileException, e :
                print( "Input file error for %s : %s" % ( i, e ))
                failures.append( i )
                if args.onerror == "fail":
                    raise
                
        if len( results ) > 0 :
            print( "Processed  : %i files" % len( results ))
        if len( failures ) > 0 :
            print( "Failed to process : %s" % failures )
