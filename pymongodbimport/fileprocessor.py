'''
Created on 24 Jul 2017

@author: jdrumgoole
'''

from pymongodbimport.fieldconfig import FieldConfig, FieldConfigException
from pymongodbimport.bulkwriter import BulkWriter
import sys

class InputFileException(Exception):
    def __init__(self, *args,**kwargs):
        Exception.__init__(self,*args,**kwargs)
        
class FileProcessor( object ):
    
    def __init__(self, collection ):
        self._collection = collection
        
    def processOneFile( self, input_filename, fieldConfig, args ):
    
        bw = BulkWriter( self._collection, input_filename, fieldConfig, args )
        totalWritten = bw.bulkWrite()
        return totalWritten 
    
    def processFiles( self, args ):
        
        totalCount = 0
        lineCount = 0
        results=[]
        failures=[]
        
        for i in args.filenames :
            if args.genfieldfile :
                if len( args.filenames ) > 0 : 
                    print( "Generating a field file from '%s'"  % args.filenames[ 0 ]  )
                    field_filename = FieldConfig.generate_field_file( args.filenames[ 0 ], args.delimiter, ext=".ff" )
                    print("Generated: '%s'" % field_filename )
                else:
                    print( "No files to generate a field file from")
                sys.exit( 0 )
            elif args.fieldfile:
                field_filename = args.fieldfile
            else:
                field_filename = FieldConfig.generate_field_filename( args.filenames[ 0 ] )
                
        
            fieldConfig = FieldConfig( field_filename,
                                       i,
                                       args.hasheader, 
                                       args.addfilename, 
                                       args.addtimestamp, 
                                       args.id)
            try:
                print ("Processing : %s" % i )
                lineCount = self.processOneFile( i, fieldConfig, args )
                totalCount = lineCount + totalCount
            except FieldConfigException, e :
                print( "Field file error for %s : %s" % ( i, e ))
                failures.append( i )
            except InputFileException, e :
                print( "Input file error for %s : %s" % ( i, e ))
                failures.append( i )
                
        if len( results ) > 0 :
            print( "Processed  : %i files" % len( results ))
        if len( failures ) > 0 :
            print( "Failed to process : %s" % failures )
