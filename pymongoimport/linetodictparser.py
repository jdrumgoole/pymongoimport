

from datetime   import datetime
import csv
from enum import Enum
import logging
from typing import List

from pymongoimport.fieldfile import FieldFile
from pymongoimport.type_converter import Converter
from pymongoimport.doctimestamp import DocTimeStamp


class ErrorResponse(Enum):
    Ignore = "ignore"
    Warn   = "warn"
    Fail   = "fail"

    def __str__(self):
        return self.value


class LineToDictParser:

    def __init__(self,
                 field_file : FieldFile,
                 locator: bool = True,
                 timestamp : DocTimeStamp = DocTimeStamp.DOC_TIMESTAMP,
                 onerror: ErrorResponse = ErrorResponse.Warn):

        self._logger = logging.getLogger(__name__)

        self._onerror = onerror
        self._record_count = 0
        self._line_count = 0
        self._timestamp = None
        self._idField = None  # section on which name == _id
        self._log = logging.getLogger(__name__)
        self._converter = Converter(self._log)
        self._field_file = field_file
        self._locator = locator
        if timestamp == DocTimeStamp.BATCH_TIMESTAMP:
            self._batch_timestamp = datetime.utcnow()

    def parse_list(self, csv_line: List[str], line_number: int)->dict:
        """
        Make a new doc from a dictEntry generated by the csv.DictReader.

        :param csv_line: the line to be parsed (list of strs)
        :param record_number: the location of the line in the input file
        :return: the new doc

        WIP
        Do we make gen id generate a compound key or another field instead of ID
        """

        doc = {}

        if len(csv_line) == 1:
            self._logger.warning("Warning: only one field in "
                                 "input line. Do you have the "
                                 "right delimiter set ?")
            self._logger.warning(f"input line : {csv_line}")

        if len(csv_line) != len(self._field_file.fields()):
            raise ValueError(f"\nrecord: at line {line_number}:{csv_line}(len={len(csv_line)}) and fields required\n"
                             f"{self._field_file.fields()}(len={len(self._field_file.fields())})"
                             f"don't match in length")

        # print( "dictEntry: %s" % dictEntry )
        field_count = 0

        for i,k in enumerate(self._field_file.fields()):
            # print( "field: %s" % k )
            # print( "value: %s" % dictEntry[ k ])
            field_count = field_count + 1

            if csv_line[i] is None:

                msg = f"Value for field '{k}' at line {line_number} is 'None' which is not valid\n"
                # print(dictEntry)
                msg = msg + f"\t\t\tline:{line_number}:'{csv_line}'"
                if self._onerror == ErrorResponse.Fail:
                    if self._log:
                        self._log.error(msg)
                    raise ValueError(msg)
                elif self._onerror == ErrorResponse.Warn:
                    if self._log:
                        self._log.warning(msg)
                    continue
                else:
                    continue

            if k.startswith("blank-") and self._onerror == ErrorResponse.Warn:  # ignore blank- columns
                if self._log:
                    self._log.info("Field %i is blank [blank-] : ignoring", field_count)
                continue

            # try:
            try:
                type_field = self._field_file.type_value(k)
                if type_field in ["date", "datetime"]:
                    fmt = self._field_file.format_value(k)
                    v = self._converter.convert_time(type_field, csv_line[i], fmt)
                else:
                    v = self._converter.convert(type_field, csv_line[i])

            except ValueError:

                if self._onerror == ErrorResponse.Fail:
                    if self._log:
                        self._log.error("Error at line %i at field '%s'", self._record_count, k)
                        self._log.error("type conversion error: Cannot convert '%s' to type %s", csv_line[i],
                                        type_field)
                    raise
                elif self._onerror == ErrorResponse.Warn:
                    msg = "Parse failure at line {} at field '{}'".format(self._record_count, k)
                    msg = msg + " type conversion error: Cannot convert '{}' to type {} using string type instead".format(
                        csv_line[i], type_field)
                    v = str(csv_line[i])
                elif self._onerror == ErrorResponse.Ignore:
                    v = str(csv_line[i])
                else:
                    raise ValueError("Invalid value for onerror: %s" % self._onerror)

            if self._field_file.has_new_name(k):
                assert (self._field_file.name_value(k) is not None)
                doc[self._field_file.name_value(k)] = v
            else:
                doc[k] = v

            if self._locator:
                doc['locator'] = {"line": line_number}

            if self._timestamp == DocTimeStamp.DOC_TIMESTAMP:
                doc['timestamp'] = datetime.utcnow()
            elif self._timestamp == DocTimeStamp.BATCH_TIMESTAMP:
                doc['timestamp'] = self._batch_timestamp

        return doc





