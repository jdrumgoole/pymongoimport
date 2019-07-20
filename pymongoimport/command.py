"""

Author: joe@joedrumgoole.com

5-May-2018

"""
import os
import logging

from pymongoimport.fieldfile import FieldFile
from pymongoimport.filewriter import FileWriter
from pymongoimport.configfile import ConfigFile
from pymongoimport.csvparser import CSVParser


class Command(object):

    def __init__(self, audit=None, id=None):
        self._name = None
        self._log = logging.getLogger(__name__)
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

    def __init__(self, database, audit=None, id=None):
        super().__init__(audit, id)
        self._name = "drop"
        self._log = logging.getLogger(__name__)
        self._database = database

    def post_execute(self, arg):
        if self._audit:
            self._audit.add_command(self._id, self.name(), {"database": self._database.name,
                                                            "collection_name": arg})
        self._log.info("dropped collection: %s.%s", self._database.name, arg)

    def execute(self, arg):
        # print( "arg:'{}'".format(arg))
        self._database.drop_collection(arg)


class GenerateFieldfileCommand(Command):

    def __init__(self, audit=None, id=None):
        super().__init__(audit, id)
        self._name = "generate"
        self._log = logging.getLogger(__name__)
        self._field_filename = None

    def field_filename(self):
        return self._field_filename

    def execute(self, arg):
        ff= FieldFile(arg)
        ff.generate_field_file()
        self._field_filename = ff.field_filename

        return self._field_filename

    def post_execute(self, arg):
        self._log.info("Creating field filename '%s' from '%s'", self._name, arg)


class ImportCommand(Command):

    def __init__(self, collection, field_filename=None, delimiter=",", hasheader=True, onerror="warn", limit=0,
                 audit=None, id=None):

        super().__init__(audit, id)

        self._log = logging.getLogger(__name__)
        self._collection = collection
        self._name = "import"
        self._field_filename = field_filename
        self._delimiter = delimiter
        self._hasheader = hasheader
        self._onerror = onerror
        self._limit = limit
        self._total_written = 0
        self._config = None

        if self._log:
            self._log.info("Auditing output")

    def pre_execute(self, arg):
        # print(f"'{arg}'")
        super().pre_execute(arg)
        if self._log:
            self._log.info("Using collection:'{}'".format(self._collection.full_name))

        if not self._field_filename:
            # print( "arg:'{}".format(arg))
            self._field_filename = FieldFile(arg).field_filename

        if self._log:
            self._log.info("Using field file:'{}'".format(self._field_filename))

        if not os.path.isfile(self._field_filename):
            raise OSError("No such field file:'{}'".format(self._field_filename))

    def execute(self, arg):

        if self._field_filename:
            field_filename = self._field_filename
        else:
            field_filename = FieldFile(arg).field_filename

        if not os.path.isfile(field_filename):
            error_msg = "The fieldfile '{}' does not exit".format(field_filename)
            self._log.error(error_msg)
            raise ValueError(error_msg)

        if self._log:
            self._log.info("using field file: '%s'", field_filename)
        self._config = ConfigFile(field_filename)
        csv_parser = CSVParser(self._config, self._hasheader, self._delimiter, self._onerror)
        self._fw = FileWriter(self._collection, csv_parser, self._limit)
        self._total_written = self._total_written + self._fw.insert_file(arg)

        return self._total_written

    def total_written(self):
        return self._total_written

    def get_config(self):
        return self._config

    def post_execute(self, arg):
        super().post_execute(arg)
        if self._audit:
            self._audit.add_command(self._id, self.name(), {"filename": arg})

        if self._log:
            self._log.info("imported file: '%s'", arg)
