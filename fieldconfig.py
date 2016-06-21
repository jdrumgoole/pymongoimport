'''
Created on 2 Mar 2016

@author: jdrumgoole
'''

from ConfigParser import RawConfigParser
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


    def __init__(self, filename = None ):
        '''
        Constructor
        '''
        self._idField = None # section on which name == _id
        self._tags = [ "name", "type", "format" ]
        self._cfg = RawConfigParser()
        self._fieldDict = OrderedDict()
        self._names = OrderedDict()
        
        if filename :
            self._fieldDict = self.read( filename )
    
    def duplicateIDMsg(self, firstSection, secondSection):
        msg = textwrap.dedent( """\
        The type defintion 'id" occurs in more that one section (there can only be one
        id definition). The first section is [%s] and the second section is [%s]
        """ )
        
        return msg % (firstSection, secondSection )
        

    def read(self, filename):
        
        idCount = 0
        firstSection = None
        secondSection = None
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
    
    
    