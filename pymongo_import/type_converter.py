import datetime
from dateutil.parser import parse as date_parse
import time

class Converter(object):

    type_fields = [ "int", "float", "str", "datetime", "date", "timestamp"]

    def __init__(self):
        self._converter = {
            "int"       : Converter.to_int,
            "float"     : Converter.to_float,
            "str"       : Converter.to_str,
            "datetime"  : Converter.to_datetime,
            "date"      : Converter.to_datetime,
            "timestamp" : Converter.to_timestamp
        }

    @staticmethod
    def to_int(v):
        try:
            # print( "converting : '%s' to int" % v )
            v = int(v)
        except ValueError:
            v = float(v)
        return v

    @staticmethod
    def to_float(v):
        return float(v)

    @staticmethod
    def to_str(v):
        return str(v)

    @staticmethod
    def to_datetime(v, format=None):
        if v == "NULL":
            return None
        elif format is None:
            return date_parse(v) #much slower than strptime, avoid for large jobs
        else:
            try:
                return datetime.datetime.strptime(v, format)
            except ValueError:
                return date_parse(v)

    @staticmethod
    def to_timestamp(v):
        return datetime.datetime.fromtimestamp(int(v))

    def convert_time(self, t, v, f=None):
        return self._converter[t](v, f)

    def convert(self, t, v):
        '''
        Use type entry for the field in the fieldConfig file (.ff) to determine what type
        conversion to use.
        '''
        try:
            return self._converter[t](v)
        except ValueError:
            v = str(v)

        return v