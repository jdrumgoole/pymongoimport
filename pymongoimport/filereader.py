import csv
from datetime import datetime
from typing import Iterator

import requests

from pymongoimport.csvparser import CSVParser
from pymongoimport.doctimestamp import DocTimeStamp


class FileReader:
    """
    Read CSV lines from a local file or a URL. Provide a generator that returns dicts of the
    lines as key->value pairs, where the keys are the column names.
    """

    UTF_ENCODING = "utf-8"
    URL_CHUNK_SIZE = 8192

    def __init__(self,
                 name: str,
                 parser: CSVParser = None,
                 has_header: bool = False,
                 delimiter : str = ",",
                 limit: int = 0):

        self._name = name
        self._parser = parser
        self._limit = limit
        self._has_header = has_header

        if delimiter == "tab":
            self._delimiter = "\t"
        else:
            self._delimiter = delimiter

    @property
    def name(self):
        return self._name

    def iterate_rows(self, iterator,
                     parser: CSVParser = None,
                     limit: int = 0) -> Iterator[object]:
        """
        Iterate rows in a presumed CSV file. If the parser object passed
        in by the constructor is not None then parse each line into a doc. Otherwise
        just return the raw line.

        :param iterator: Read from this iterator
        :param parser: The CSV parser. may be none.
        :param limit: Only read up to limit lines (0 for all lines)
        :return: An iterator providing parsed lines.
        """

        if self._has_header:
            next(iterator)

        # size = 0
        for processed_lines, row in enumerate(iterator, 1):
            if limit > 0:
                if processed_lines > limit:
                    break
            if parser:
                doc = parser.parse_csv_line(row, processed_lines)
                yield doc
            else:
                yield row

    def read_file(self, limit: int = 0) -> object:
        if self._name.startswith("http"):
            yield from self.read_url_file(limit=limit)
        else:
            yield from self.read_local_file(limit=limit)

    @staticmethod
    def read_remote_by_line(url: str) -> Iterator[str]:
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            for line in r.iter_lines():
                if line:
                    decoded_line = line.decode(FileReader.UTF_ENCODING)
                    yield decoded_line

    def read_url_file(self,
                      limit: int = 0) -> Iterator[object]:
        reader = csv.reader(FileReader.read_remote_by_line(self._name),
                            delimiter=self._delimiter)
        yield from self.iterate_rows(reader, self._parser, limit)

    def read_local_file(self, limit: int = 0) -> Iterator[object]:

        with open(self._name, newline="") as csv_file:
            reader = csv.reader(csv_file, delimiter=self._delimiter)
            yield from self.iterate_rows(reader, self._parser, limit)
