"""
Created on 2 Mar 2016

@author: jdrumgoole
"""

import os

from configparser import RawConfigParser

from pymongoimport.type_converter import Converter


def dict_to_fields(d):
    f = []
    for k, v in d.items():
        if type(v) == dict:
            f.extend(dict_to_fields(v))
        else:
            f.append(k)
    return f


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

    def __init__(self, name):

        self._name = name
        self._cfg = RawConfigParser()
        self._fields = None
        self._field_dict = {}
        self._idField = None
        self._tags = ["name", "type", "format"]

        if os.path.exists(self._name):
            self.read(self._name)
        else:
            raise OSError(f"No such file {self._name}")

    @staticmethod
    def make_default_ff_name(name):
        return f"{os.path.splitext(name)[0]}.ff"

    @property
    def field_filename(self):
        return self._name

    @staticmethod
    def generate_field_file(csv_filename, ff_filename=None, ext=".ff", delimiter=","):

        if not ext.startswith("."):
            ext = f".{ext}"

        if ff_filename is None:
            ff_filename = os.path.splitext(csv_filename)[0] + ext

        with open(ff_filename, "w") as ff_file:
            # print( "The field file will be '%s'" % genfilename)
            with open(csv_filename, "r") as input_file:
                column_names = input_file.readline().rstrip().split(delimiter)  # strip newline
                column_values = input_file.readline().rstrip().split(delimiter)
                if len(column_names) > len(column_values):
                    raise ValueError(f"Header line has more columns than first line: {len(column_names)} > {len(column_values)}")
                elif len(column_names) < len(column_values):
                    raise ValueError(f"Header line has less columns than first line: {len(column_names)} < {len(column_values)}")

            for i, name in enumerate(column_names):

                if name == "":
                    name = f"blank-{i}"
                # print( i )

                name = name.strip()  # strip out white space
                if name.startswith('"'): # strip out quotes if they exist
                    name = name.strip('"')
                if name.startswith("'"):
                    name = name.strip("'")
                name = name.replace('$', '_')  # not valid keys for mongodb
                name = name.replace('.', '_')  # not valid keys for mongodb
                t = Converter.guess_type(column_values[i])
                ff_file.write(f"[{name}]\n")
                ff_file.write(f"type={t}\n")

        return FieldFile(ff_filename)

    def read(self, filename):

        result = self._cfg.read(filename)
        if len(result) == 0:
            raise OSError("Couldn't open '{}'".format(filename))

        self._fields = self._cfg.sections()

        for s in self._fields:
            # print( "section: '%s'" % s )
            self._field_dict[s] = {}
            for o in self._cfg.options(s):
                # print("option : '%s'" % o )
                if not o in self._tags:
                    raise ValueError("No such field type: %s in section: %s" % (o, s))
                if (o == "name"):
                    if (self._cfg.get(s, o) == "_id"):
                        if self._idField == None:
                            self._idField = s
                        else:
                            raise ValueError("Duplicate _id field:{} and {}".format(self._idField, s))

                self._field_dict[s][o] = self._cfg.get(s, o)

            if not "name" in self._field_dict[s]:
                # assert( s != None)
                self._field_dict[s]["name"] = s
            #
            # format is optional for datetime input fields. It is used if present.
            #
            if not "format" in self._field_dict[s]:
                self._field_dict[s]["format"] = None

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
