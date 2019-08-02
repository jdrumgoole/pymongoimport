"""

Author: joe@joedrumgoole.com

5-May-2018

"""
import os
import logging

import pymongo

from pymongoimport.fieldfile import FieldFile
from pymongoimport.filewriter import FileWriter
from pymongoimport.csvparser import CSVParser
from pymongoimport.csvparser import ErrorResponse
from pymongoimport.filereader import FileReader
from pymongoimport.doctimestamp import DocTimeStamp


class Command:

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

    def __init__(self, audit=None, id=None,delimiter=","):
        super().__init__(audit, id)
        self._name = "generate"
        self._log = logging.getLogger(__name__)
        self._field_filename = None
        self._delimiter = delimiter

    def field_filename(self):
        return self._field_filename

    def execute(self, arg):
        ff = FieldFile.generate_field_file(arg)
        self._field_filename = ff.field_filename
        return self._field_filename

    def post_execute(self, arg):
        self._log.info(f"Created field filename \n'{self._field_filename}' from '{arg}'")


class ImportCommand(Command):

    def __init__(self,
                 collection:pymongo.collection,
                 field_filename: str = None,
                 delimiter:str = ",",
                 has_header:bool = True,
                 onerror: ErrorResponse = ErrorResponse.Warn,
                 limit: int = 0,
                 locator=False,
                 timestamp: DocTimeStamp = DocTimeStamp.NO_TIMESTAMP,
                 audit:bool= None,
                 id:object= None):

        super().__init__(audit, id)

        self._log = logging.getLogger(__name__)
        self._collection = collection
        self._name = "import"
        self._field_filename = field_filename
        self._fieldinfo = None
        self._delimiter = delimiter
        self._has_header = has_header
        self._parser = None
        self._reader = None
        self._writer = None
        self._onerror = onerror
        self._limit = limit
        self._locator = locator
        self._timestamp = timestamp
        self._total_written = 0
        self._config = None

        self._log.info("Auditing output")

    def pre_execute(self, arg):
        # print(f"'{arg}'")
        super().pre_execute(arg)
        self._log.info("Using collection:'{}'".format(self._collection.full_name))

        if self._field_filename is None:
            self._field_filename = FieldFile.make_default_ff_name(arg)

        self._log.info(f"Using field file:'{self._field_filename}'")

        if not os.path.isfile(self._field_filename):
            raise OSError(f"No such field file:'{self._field_filename}'")

        self._fieldinfo = FieldFile(self._field_filename)
        self._parser = CSVParser(self._fieldinfo,
                                 self._has_header,
                                 self._delimiter,
                                 self._onerror)
        self._reader = FileReader(arg,
                                  parser=self._parser,
                                  limit=self._limit,
                                  locator=self._locator,
                                  timestamp=self._timestamp)
        self._writer = FileWriter(self._collection, self._reader)

    def execute(self, arg):

        self._total_written = self._writer.write()

        return self._total_written

    def total_written(self):
        return self._total_written

    @property
    def fieldinfo(self):
        return self._fieldinfo

    def post_execute(self, arg):
        super().post_execute(arg)
        if self._audit:
            self._audit.add_command(self._id, self.name(), {"filename": arg})

        if self._log:
            self._log.info("imported file: '%s'", arg)
