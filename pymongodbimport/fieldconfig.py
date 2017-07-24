'''
Created on 2 Mar 2016

@author: jdrumgoole
'''

from ConfigParser import RawConfigParser
from datetime import datetime
import csv
import os

import textwrap
from collections import OrderedDict

class FieldConfigException(Exception):
    def __init__(self, *args,**kwargs):
        Exception.__init__(self,*args,**kwargs)
          

class FieldConfig(object):
    '''
      Each field is represented by a section in the config parser
      For each field there are a set of configurations:
      
      type = the type of this field, int, float, str, date, 
      format = the way the content will be formatted for now really only used to date
      name = an optional name field. If not present the section name will be used.
      
      If the name field is "_id" then this will be used as the _id field in the collection.
      Only one name =_id can be present in any fieldConfig file.
      
      The values in this column must be unique in the source data file otherwise loading will fail
      with a duplicate key error.
      
    '''


    def __init__(self, cfgFilename = None, input_filename = None, hasheader=None, addfilename=None, addtimestamp=None, _id=None ):
        '''
        Constructor
        '''
        self._idField = None # section on which name == _id
        self._hasheader = hasheader
        self._tags = [ "name", "type", "format" ]
        self._cfg = RawConfigParser()
        self._fieldDict = OrderedDict()
        self._names = OrderedDict()
        self._doc_template = OrderedDict()
        self._id = _id
        self._filename = input_filename
        self._record_count = 0
        self._timestamp = addtimestamp
        self._pid = os.getpid()
        
        if cfgFilename :
            self._fieldDict = self.read( cfgFilename )
            
        if addfilename:
            self._doc_template[ "filename"] = os.path.basename( self._filename )
 
        if addtimestamp == "now" :
            self._doc_template[ "timestamp" ] = self._timestamp
            
    
    def duplicateIDMsg(self, firstSection, secondSection):
        msg = textwrap.dedent( """\
        The type defintion 'id" occurs in more that one section (there can only be one
        id definition). The first section is [%s] and the second section is [%s]
        """ )
        
        return msg % (firstSection, secondSection )
        

    def hasheader(self ):
        return self._hasheader
    
    def read(self, filename):
        
        fieldDict = OrderedDict()
        result = self._cfg.read( filename )
        if len( result ) == 0 :
            raise ValueError( "Couldn't open %s" % filename )

        self._fields = self._cfg.sections()
        
        for s in self._fields :
            #print( "section: '%s'" % s )
            fieldDict[ s ] = {}
            for o in self._cfg.options( s ):
                #print("option : '%s'" % o )
                if not o in self._tags :
                    raise ValueError( "No such field type: %s in section: %s" % (o, s ))
                if ( o == "name" ):
                    if ( self._cfg.get( s, o ) == "_id" ) :
                        if self._idField == None :
                            self._idField = s
                        else:
                            raise ValueError( self.duplicateIDMsg( self._idField, s ))
                    
                fieldDict[ s ][ o ] = self._cfg.get( s, o )
                
            if not "name" in fieldDict[ s ]: 
                fieldDict[ s ]["name"] = s
            
        self._fieldDict = fieldDict
        return fieldDict 
            
    def fieldDict(self):
        if self._fieldDict is None :
            raise ValueError( "trying retrieve a fieldDict which has a 'None' value" )
        else:
            return self._fieldDict

    def fields(self):
        return self._fields
    
    def hasNewName(self, section ):
        return section != self._fieldDict[ section ]['name']
    
    def names(self):
        return self._names 

    def typeData(self, fieldName ):
        return self._cfg.get( fieldName, "type")
    
    def formatData(self, fieldName ):
        return self._cfg.get( fieldName, "format")
    
    def nameData(self , fieldName ):
        return self._cfg.get( fieldName, "name" )
    
#     @staticmethod
#     def generateFieldFile( csvfile, delimiter=',' ):
#         
#         with open( csvfile ) as inputfile :
#             header = inputfile.readline()
#             
#         for field in header.split(  delimiter ):
#             print( field )
            
    @staticmethod
    def typeConvert( s ):
        '''
        Try and convert a string s to an object. Start with float, then try int
        and if that doesn't work return the string.
        '''
        
        if type( s ) != type( "" ):
            raise ValueError( "typeconvert expects a string parameter")
        
        v = None
        try :
            v = int( s )
            return ( v, "int" ) 
        except ValueError :
            pass
        
        try :
            v = float( s )
            return (v, "float")
        except ValueError :
            pass
        
        v = str( s )
        return ( v, "str" )
        
    def doc_template(self):
        return self._doc_template
    
    def createDoc( self, dictEntry  ):
        
        '''
        WIP
        Do we make gen id generate a compound key or another field instead of ID
        '''
        self._record_count = self._record_count + 1 
        
        doc = {}
        doc.update( self._doc_template )
        
        if self._id == "gen" :
            doc[ "_id"] = "%i-%i" % ( self._pid, self._record_count )
            
        if self._timestamp == "gen" :
            doc[ 'timestamp' ] = datetime.utcnow()
            
        #print( dictEntry )
        for k in self.fields() :
            if k == "" or k == "blank":
                # blank column, ignore
                continue
            try:
                typeField = self.typeData( k )
                try: 
                    if typeField == "int" :
                        v = int( dictEntry[ k ])
                    elif typeField == "float" :
                        v = float( dictEntry[ k ])
                    elif typeField == "str" :
                        v = str( dictEntry[ k ])
                    elif typeField == "date":
                        if dictEntry[ k ] == "NULL" :
                            v = 'NULL'
                        else:
                            v = datetime.strptime( dictEntry[ k ], self.formatData( k ) )
                except ValueError:
                    #print( "Error on line %i at field '%s'" % ( self._record_count, k ))
                    #print("type conversion error: Cannot convert %s to type %s" % (dictEntry[ k ], typeField))
                    #print( "Using string type instead")
                    v = str( dictEntry[ k ])
                    
                if self.hasNewName( k ):
                    doc[ self.nameData( k )] = v
                else:
                    doc[ k ] = v
                        
            except ValueError :
                print( "Value error parsing field : [%s]" % k )
                print( "read value is: '%s'" % dictEntry[ k ] )
                print( "line: %i, '%s'" %  ( self._record_count, dictEntry ))
                #print( "ValueError parsing filed : %s with value : %s (type of field: $s) " % ( str(k), str(line[ k ]), str(fieldDict[ k]["type"])))
                raise   
    
        return doc

    @staticmethod
    def generate_field_filename( path, ext=".ff"):
        if not os.path.isfile( path) :
            raise OSError( "no such field file '%s'"  % path)
        
        if not ext.startswith( '.'):
            ext = "." + ext
        return os.path.splitext( os.path.basename( path ))[0] + ext
        
    @staticmethod
    def generate_field_file( path, delimiter=",", ext=".ff"):
        '''
        Take a file name and create a field file name and the corresponding field file data 
        from that file by reading the headers and 'sniffing' the first line of data.
        '''
        
        genfilename = FieldConfig.generate_field_filename(path, ext)
        
        with open( genfilename, "w") as genfile :
            #print( "The field file will be '%s'" % genfilename )
            with open( path, "rU") as inputfile :
                header_line = inputfile.readline().rstrip().split( delimiter ) #strip newline
                value_line  = inputfile.readline().rstrip().split( delimiter )
                if len( header_line ) != len( value_line ):
                    raise ValueError( "Header line and next line have different numbers of columns: %i, %i" % ( len( header_line ), len( value_line )))
            value_index = 0
            for i in header_line :
                if i == "" :
                    i = "blank"
                #print( i )
                i = i.replace( '$', '_') # not valid keys for mongodb
                i = i.replace( '.', '_') # not valid keys for mongodb
                ( _, t ) = FieldConfig.typeConvert( value_line[ value_index ] )
                value_index = value_index + 1 
                genfile.write( "[%s]\n" % i )
                genfile.write( "type=%s\n" % t )
                                
        return genfilename