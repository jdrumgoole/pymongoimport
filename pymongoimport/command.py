"""

Author: joe@joedrumgoole.com

5-May-2018

"""
import os

from pymongoimport.fieldconfig import FieldConfig
from pymongoimport.file_writer import File_Writer


class Command(object):

    def __init__(self, log, audit=None, id=None):
        self._name = None
        self._log = log
        self._audit = audit
        self._id = id

    def name(self):
        return self._name

    def pre_execute(self, arg):
        pass

    def execute(self, arg):
        pass

    def post_execute(self, arg):
        pass

    def run(self, *args):
        for i in args:
            self.pre_execute(i)
            self.execute(i)
            self.post_execute(i)


class Drop_Command(Command):

    def __init__(self, log, database, audit=None, id=None):
        super().__init__(log, audit, id)
        self._name = "drop"
        self._database = database

    def post_execute(self, arg):
        if self._audit:
            self._audit.add_command(self._id, self.name(), {"database": self._database.name,
                                                            "collection_name": arg})
        self._log.info("dropped collection: %s.%s", self._database.name, arg)

    def execute(self, arg):
        # print( "arg:'{}'".format(arg))
        self._database.drop_collection(arg)


class Generate_Fieldfile_Command(Command):

    def __init__(self, log, delimiter, audit=None, id=None):
        super().__init__(log, audit, id)
        self._delimiter = delimiter
        self._name = "generate"
        self._fieldfile_names = []

    def names(self):
        return self._field_file_names

    def execute(self, arg):
        self._name = FieldConfig.generate_field_file(arg, self._delimiter)
        self._fieldfile_names.append(self._name)

        return self._name

    def post_execute(self, arg):
        self._log.info("Creating field filename '%s' from '%s'", self._name, arg)


class Import_Command(Command):

    def __init__(self, log, collection, field_filename=None, delimiter=",", hasheader=True, onerror="warn", limit=0,
                 audit=None, id=None):

        super().__init__(log, audit, id)
        self._collection = collection
        self._name = "import"
        self._field_filename = field_filename
        self._delimiter = delimiter
        self._hasheader = hasheader
        self._onerror = onerror
        self._limit = limit
        self._total_written = 0
        self._fieldConfig = None

        if self._log:
            self._log.info("Auditing output")

    def pre_execute(self, arg):
        # print(f"'{arg}'")
        super().pre_execute(arg)
        if self._log:
            self._log.info("Using collection:'{}'".format(self._collection.full_name))

        if not self._field_filename:
            # print( "arg:'{}".format(arg))
            self._field_filename = FieldConfig.generate_field_filename(arg)

        if self._log:
            self._log.info("Using field file:'{}'".format(self._field_filename))

        if not os.path.isfile(self._field_filename):
            raise OSError("No such field file:'{}'".format(self._field_filename))

    def execute(self, arg):

        if self._field_filename:
            field_filename = self._field_filename
        else:
            field_filename = FieldConfig.generate_field_filename(arg)

        if not os.path.isfile(field_filename):
            error_msg = "The fieldfile '{}' does not exit".format(field_filename)
            self._log.error(error_msg)
            raise ValueError(error_msg)

        if self._log:
            self._log.info("using field file: '%s'", field_filename)
        self._fieldConfig = FieldConfig(self._log,
                                        field_filename,
                                        self._delimiter,
                                        self._hasheader,
                                        self._onerror)

        self._fw = File_Writer(self._collection, self._fieldConfig, self._limit, self._log)
        self._total_written = self._total_written + self._fw.insert_file(arg)

        return self._total_written

    def total_written(self):
        return self._total_written

    def get_field_config(self):
        return self._fieldConfig

    def post_execute(self, arg):
        super().post_execute(arg)
        if self._audit:
            self._audit.add_command(self._id, self.name(), {"filename": arg})

        if self._log:
            self._log.info("imported file: '%s'", arg)
