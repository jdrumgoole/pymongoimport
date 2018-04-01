'''
Created on 2 Mar 2016

@author: jdrumgoole
'''

from ConfigParser import RawConfigParser
from datetime import datetime
import os
import csv
import logging
import textwrap
from dateutil.parser import parse

from pymongodbimport.logger import Logger


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

    def __init__(self, cfgFilename, delimiter=",", hasheader=True, gen_id="mongodb", onerror="warn"):
        '''
        Constructor
        '''
        self._logger = logging.getLogger( Logger.LOGGER_NAME  )
        self._idField = None # section on which name == _id
        self._tags = [ "name", "type", "format" ]
        self._cfg = RawConfigParser()
        self._fieldDict = OrderedDict()
        self._names = OrderedDict()
        self._doc_template = OrderedDict()
        self._id = gen_id
        self._delimiter = delimiter
        self._record_count = 0
        self._line_count = 0
        self._timestamp = None
        self._pid = os.getpid()
        self._onerror = onerror
        self._hasheader = hasheader
        
        if cfgFilename :
            self._fieldDict = self.read( cfgFilename )
            
            
    def hasheader(self):
        return self._hasheader
    
    def add_timestamp(self, timestamp ):
        '''
        timestamp = "now" generate time once for all docs
        timestamp = "gen" generate a new timestamp for each doc
        timestamp = "none" don't add timestamp field
        '''
        self._timestamp = timestamp 
        if timestamp == "now":
            self._doc_template[ "timestamp" ] = datetime.utcnow()
        return self._doc_template
            
    def add_filename(self, filename ):
        self._doc_template[ "filename"] = os.path.basename( filename )
        return self._doc_template
    
    def get_dict_reader(self, f ):
        return csv.DictReader( f, fieldnames = self.fields(), delimiter = self._delimiter )
    
    def duplicateIDMsg(self, firstSection, secondSection):
        msg = textwrap.dedent( """\
        The type defintion '_id" occurs in more that one section (there can only be one
        _id definition). The first section is [%s] and the second section is [%s]
        """ )
        
        return msg % (firstSection, secondSection )
    
    def delimiter(self):
        return self._delimiter
    
    def read(self, filename):
        '''
        Read fieldfile values into a dictionary without type conversion
        '''
        
        fieldDict = OrderedDict()
        
        result = self._cfg.read( filename )
        if len( result ) == 0 :
            raise FieldConfigException( "Couldn't open '%s'" % filename )

        self._fields = self._cfg.sections()
        
        for s in self._fields :
            #print( "section: '%s'" % s )
            fieldDict[ s ] = {}
            for o in self._cfg.options( s ):
                #print("option : '%s'" % o )
                if not o in self._tags :
                    raise FieldConfigException( "No such field type: %s in section: %s" % (o, s ))
                if ( o == "name" ):
                    if ( self._cfg.get( s, o ) == "_id" ) :
                        if self._idField == None :
                            self._idField = s
                        else:
                            raise FieldConfigException( self.duplicateIDMsg( self._idField, s ))
                    
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
    def guess_type( s ):
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
        
        try :
            v = parse( s ) #dateutil.parse.parser
            return ( v, "datetime" )
        except ValueError:
            pass
        
        v = str( s )
        return ( v, "str" )
        
    def doc_template(self):
        return self._doc_template
    
    def type_convert(self, v, t ):
        '''
        Use type entry for the field in the fieldConfig file (.ff) to determine what type
        conversion to use.
        '''
        v = v.strip()
        
        if t == "timestamp" :
            v = datetime.datetime.fromtimestamp(int( v ))
        elif t == "int" : #Ints can be floats
            try :
                #print( "converting : '%s' to int" % v )
                v = int(v)
            except ValueError :
                v = float( v )
        elif t == "float" :
            v = float( v )
        elif t == "str" :
            v = str( v )
        elif t == "datetime" or t == "date":
            if v == "NULL" :
                v = None
            else:
                v = parse( v )
        else:
            raise ValueError
        
        return v
    
    def createDoc(self, dictEntry):
        
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
            
        #print( "dictEntry: %s" % dictEntry )
        fieldCount = 0
        for k in self.fields() :
            #print( "field: %s" % k )
            #print( "value: %s" % dictEntry[ k ])
            fieldCount = fieldCount + 1
            
            if dictEntry[ k ] is None:
                if self._hasheader :
                    self._line_count = self._record_count + 1
                else:
                    self._line_count = self._record_count
                #self._logger.warn( "value for field '%s' at line %i is None which is not valid", k, self._line_count )
                raise ValueError( "value for field '%s' at line %i is None which is not valid (wrong delimiter?)" % (k, self._line_count))
            if k.startswith( "blank-" ) and self._onerror == "warn" : #ignore blank- columns
                self._logger.info( "Field %i is blank [blank-] : ignoring", fieldCount )
                continue
            
            #try:
            try :
                type_field  = self.typeData( k )
                v = self.type_convert( dictEntry[ k ], type_field )

            except ValueError:
                
                if self._onerror == "fail" :
                    self._logger.error( "Error at line %i at field '%s'", self._record_count, k )
                    self._logger.error("type conversion error: Cannot convert '%s' to type %s", dictEntry[ k ], type_field )
                    raise
                elif self._onerror == "warn" :
                    self._logger.info( "Parse failure at line %i at field '%s'", self._record_count, k )
                    self._logger.info( "type conversion error: Cannot convert '%s' to type %s", dictEntry[ k ], type_field )
                    self._logger.info( "Using string type instead" )
                    v = str( dictEntry[ k ])
                elif self._onerror == "ignore":
                    v = str( dictEntry[ k ])
                else:
                    raise ValueError( "Invalid value for onerror: %s" % self._onerror )
                
            if self.hasNewName( k ):
                doc[ self.nameData( k )] = v
            else:
                doc[ k ] = v
                    
#             except ValueError :
#                 self._logger.error( "Value error parsing field : [%s]" , k )
#                 self._logger.error( "read value is: '%s'", dictEntry[ k ] )
#                 self._logger.error( "line: %i, '%s'", self._record_count, dictEntry )
#                 #print( "ValueError parsing filed : %s with value : %s (type of field: $s) " % ( str(k), str(line[ k ]), str(fieldDict[ k]["type"])))
#                 raise   
    
        return doc

    @staticmethod
    def generate_field_filename( path, ext=".ff"):
        if not os.path.isfile( path) :
            raise OSError( "no such field file '%s'"  % path)
        
        if not ext.startswith( '.'):
            ext = "." + ext
        return os.path.splitext( path )[0] + ext
        
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
            fieldCount = 0
            for i in header_line :

                if i == "" :
                    i = "blank-%i" % fieldCount
                #print( i )
                
                i = i.strip() # strip out white space
                if i.startswith( '"' ) :
                    i = i.strip( '"' )
                if i.startswith( "'" ) :
                    i = i.strip( "'" )
                i = i.replace( '$', '_') # not valid keys for mongodb
                i = i.replace( '.', '_') # not valid keys for mongodb
                ( _, t ) = FieldConfig.guess_type( value_line[ fieldCount ] )
                fieldCount = fieldCount + 1
                genfile.write( "[%s]\n" % i )
                genfile.write( "type=%s\n" % t )
                                
        return genfilename
