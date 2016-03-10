'''
Created on 2 Mar 2016

@author: jdrumgoole
'''

from ConfigParser import RawConfigParser
import textwrap
from collections import OrderedDict

class FieldConfig(object):
    '''
      Each field is represented by a section in the config parser
      For each field there are a set of configurations:
      
      type = the type of this field, int, float, str, date, 
      format = the way the content will be formatted for now really only used to date
      _id = Will this be used as a ID field (column must be unique)
    '''


    def __init__(self, filename = None ):
        '''
        Constructor
        '''
        self._cfg = RawConfigParser()
        self._fieldDict = OrderedDict() 
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

        for s in self._cfg.sections() :
            #print( "section: '%s'" % s )
            fieldDict[ s ] = {}
            for o in self._cfg.options( s ):
                #print("option : '%s'" % o )
                if o == "id" :
                    #print( "id found")
                    if idCount == 0 :
                        idCount = idCount + 1 ;
                        firstSection = s
                    else :
                        secondSection = s
                        raise ValueError( self.duplicateIDMsg( firstSection, secondSection ))
                    
                fieldDict[ s ][ o ] = self._cfg.get( s, o )
                
        return fieldDict 
            
    def fieldDict(self):
        if self._fieldDict is None :
            raise ValueError( "trying retrieve a fieldDict which has a 'None' value" )
        else:
            return self._fieldDict
        
    def fields(self):
        return self._cfg.sections()
    
    def typeData(self, fieldName ):
        return self._cfg.get( fieldName, "type")
    
    def formatData(self, fieldName ):
        return self._cfg.get( fieldName, "format")
    
    def idData(self , fieldName ):
        return self._cfg.get( fieldName, "id" )
        pass
    
    
    