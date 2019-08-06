"""
Created on 2 Mar 2016

@author: jdrumgoole
"""

import os
import toml
from enum import Enum
from datetime import datetime
from configparser import RawConfigParser

from pymongoimport.type_converter import Converter

class FieldFileException(Exception):
    pass

def dict_to_fields(d):
    f = []
    for k, v in d.items():
        if type(v) == dict:
            f.extend(dict_to_fields(v))
        else:
            f.append(k)
    return f


class FieldNames(Enum):
    NAME = "name"
    TYPE = "type"
    FORMAT = "format"

    def __str__(self):
        return self.value


class FieldFile(object):
    """
      Each field is represented by a section in the config parser
      For each field there are a set of configurations:

      type = the type of this field, int, float, str, date,
      format = the way the content will be formatted for now really only used to date
      name = an optional name field. If not present the section name will be used.

      If the name field is "_id" then this will be used as the _id field in the collection.
      Only one name =_id can be present in any fieldConfig file.

      The values in this column must be unique in the source data file otherwise loading will fail
      with a duplicate key error.

      YAML
      =====

      Each field is represented by a top level field name. Each field has a nested dict
      called `_config`. That config defines the following values for the field:

        type : int|str|bool|float|datetime|dict
        format : <a valid format string for the type this field is optional>
        <other nested fields> :
            _config : <as above>
            format  : <as above>
            <other nested fields>:
              _config : <as above>
              format  : <as above>

    """

    DEFAULT_EXTENSION=".tff"

    def __init__(self, name):

        self._name = name
        self._cfg = RawConfigParser()
        self._fields = None
        self._field_dict = {}
        self._idField = None

        if os.path.exists(self._name):
            self.read(self._name)
        else:
            raise OSError(f"No such file {self._name}")

    @staticmethod
    def make_default_tff_name(name):
        return f"{os.path.splitext(name)[0]}{FieldFile.DEFAULT_EXTENSION}"

    @property
    def field_filename(self):
        return self._name

    @staticmethod
    def generate_field_file(csv_filename, ff_filename=None, ext=DEFAULT_EXTENSION, delimiter=","):

        if not ext.startswith("."):
            ext = f".{ext}"

        if ff_filename is None:
            ff_filename = os.path.splitext(csv_filename)[0] + ext

        with open(csv_filename, "r") as input_file:

            # print( "The field file will be '%s'" % genfilename)

            column_names = input_file.readline().rstrip().split(delimiter)  # strip newline
            column_values = input_file.readline().rstrip().split(delimiter)
            if len(column_names) > len(column_values):
                raise ValueError(f"Header line has more columns than first "
                                 "line: {len(column_names)} > {len(column_values)}")
            elif len(column_names) < len(column_values):
                raise ValueError(f"Header line has less columns"
                                 "than first line: {len(column_names)} < {len(column_values)}")

            toml_dict = {}
            for i, name in enumerate(column_names):
                name=name.strip()
                if name == "":
                    name = f"blank-{i}"
                # print( i )

                if name.startswith('"'):  # strip out quotes if they exist
                    name = name.strip('"')
                if name.startswith("'"):
                    name = name.strip("'")
                name = name.replace('$', '_')  # not valid keys for mongodb
                name = name.replace('.', '_')  # not valid keys for mongodb
                t = Converter.guess_type(column_values[i])
                toml_dict[name] = {}
                toml_dict[name]["type"] = t
                toml_dict[name]["name"] = name
                # ff_file.write(f"[{name}]\n")
                # ff_file.write(f"type={t}\n")
                # ff_file.write(f"name={name}")

        with open(ff_filename, "w") as ff_file:
            #print(toml_dict)
            toml_string = toml.dumps(toml_dict)
            ff_file.write("#\n")
            ts=datetime.utcnow()
            ff_file.write(f"# Created at UTC:{ts} by {__name__}\n")
            ff_file.write("#\n")
            ff_file.write(toml_string)
            ff_file.write(f"#end\n")

        return FieldFile(ff_filename)

    def read(self, filename):

        toml_data = ""

        if not os.path.exists(filename):
            raise OSError(f"No such TOML file: '{filename}'")
        with open(filename) as toml_file:
            for i in toml_file.readlines():
                toml_data = toml_data + i

        try :
            toml_dict = toml.loads(toml_data)
        except toml.decoder.TomlDecodeError as e:
            raise FieldFileException( f"Error: \'{e}\' in {filename}")
        # result = self._cfg.read(filename)

        if len(toml_data) == 0:
            raise OSError(f"No data in TOML file: '{filename}'")

        self._fields = [k for k in toml_dict.keys()]

        #print(toml_dict)
        for column_name, column_value in toml_dict.items():
            # print( "section: '%s'" % s )
            for field_name, field_value in column_value.items():
                # print("option : '%s'" % o )
                if FieldNames[field_name.upper()] not in FieldNames:
                    raise ValueError(f"Invalid field name: {field_name} in section: {column_name}")
                if field_name == "name":
                    if field_value == "_id":
                        if self._idField is None:
                            self._idField = column_name
                        else:
                            raise ValueError(f"Duplicate _id field:{column_name} appears more than once as _id")

            if not "name" in column_value.keys():
                toml_dict[column_name]["name"] = column_name
            #
            # format is optional for datetime input fields. It is used if present.
            #
            if not "format" in column_value.keys():
                toml_dict[column_name]["format"] = None

        self._field_dict = toml_dict

        return self._field_dict

    @property
    def field_dict(self):
        if self._field_dict is None:
            raise ValueError("trying retrieve a field_dict which has a 'None' value")
        else:
            return self._field_dict

    def fields(self):
        return self._fields

    def has_new_name(self, section):
        return section != self._field_dict[section]['name']

    def type_value(self, fieldName):
        return self._field_dict[fieldName]["type"]
        # return self._cfg.get(fieldName, "type")

    def format_value(self, fieldName):
        return self._field_dict[fieldName]["format"]
        # return self._cfg.get(fieldName, "format")

    def name_value(self, fieldName):
        return self._field_dict[fieldName]["name"]
        # return self._cfg.get(fieldName, "name")

    def __repr__(self):
        return f"filename:{self._name}\ndict:\n{self._field_dict}\n"