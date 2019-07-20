"""
Created on 2 Mar 2016

@author: jdrumgoole
"""

from pymongoimport.type_converter import Converter

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

    def __init__(self, csv_filename: str, ext: str = ".ff"):

        self._csv_filename = csv_filename

        if ext.startswith('.'):
            self._ext = ext
        else:
            self._ext = "." + ext

        pieces = csv_filename.split(".")
        self._ff_name = f"{pieces[0]}{self._ext}"

    @property
    def field_filename(self):
        return self._ff_name


    # def add_filename(self, filename):
    #     self._doc_template["filename"] = os.path.basename(filename)
    #     return self._doc_template



    # def duplicateIDMsg(self, firstSection, secondSection):
    #     msg = textwrap.dedent("""\
    #     The type defintion '_id" occurs in more that one section (there can only be one
    #     _id definition). The first section is [%s] and the second section is [%s]
    #     """)
    #
    #     return msg % (firstSection, secondSection)


    # def doc_template(self):
    #     return self._doc_template

    # def type_convert(self, v, t):
    #     '''
    #     Use type entry for the field in the fieldConfig file (.ff) to determine what type
    #     conversion to use.
    #     '''
    #     v = v.strip()
    #
    #     if t == "timestamp":
    #         v = datetime.datetime.fromtimestamp(int(v))
    #     elif t == "int":  # Ints can be floats
    #         try:
    #             # print( "converting : '%s' to int" % v )
    #             v = int(v)
    #         except ValueError:
    #             v = float(v)
    #     elif t == "float":
    #         v = float(v)
    #     elif t == "str":
    #         v = str(v)
    #     elif t == "datetime" or t == "date":
    #         if v == "NULL":
    #             v = None
    #         else:
    #             v = parse(v)
    #     else:
    #         raise ValueError
    #
    #     return v


    def generate_field_file(self, delimiter=","):
        """
        Create a default filed file using the data from the file.
        :param path: CSV file to use for input
        :param delimiter: delimiter character used to seperate fields in CSV file
        :param ext: File extension for field file.
        :return: The name of the generated file
        """

        with open(self._ff_name, "w") as ff_file:
            # print( "The field file will be '%s'" % genfilename)
            with open(self._csv_filename, "r") as input_file:
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

        return self._ff_name
