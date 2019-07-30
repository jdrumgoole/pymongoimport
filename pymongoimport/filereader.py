import csv
from datetime import datetime
from typing import Generator, Union, Iterator

import requests

from pymongoimport.csvparser import CSVParser
from pymongoimport.doctimestamp import DocTimeStamp


class FileReader:
    """
    Read CSV lines from a local file or a URL. Provide a generator that returns dicts of the
    lines as key->value pairs, where the keys are the column names.
    """

    def __init__(self,
                 name : str,
                 parser : CSVParser,
                 parse_doc : bool= True,
                 locator : bool = False,
                 timestamp : DocTimeStamp = DocTimeStamp.NO_TIMESTAMP,
                 limit:int = 0):

        self._name = name
        self._parser = parser
        self._parse_doc = parse_doc
        self._locator = locator
        self._timestamp = timestamp
        self._limit = limit
        self._batch_timestamp = None

        if self._timestamp == DocTimeStamp.BATCH_TIMESTAMP:
            self._batch_timestamp = datetime.utcnow()

    @property
    def name(self):
        return self._name

    def iterate_rows(self, iterator, limit:int= 0, raw:bool= False)->Generator[str, None, None]:

        for i, row in enumerate(iterator, 1):
            if limit > 0:
                if i >= limit:
                    break

            if self._parser.hasheader() and i == 1:  # skip header line
                continue

            if raw:
                yield row
            else:
                doc = self._parser.parse_csv_line(row, i)
                if self._locator:
                    doc['locator'] = {"f": self._name, "n": i + 1}
                if self._timestamp == DocTimeStamp.DOC_TIMESTAMP:
                    doc['timestamp'] = datetime.utcnow()
                elif self._timestamp == DocTimeStamp.BATCH_TIMESTAMP:
                    doc['timestamp'] = self._batch_timestamp
                yield doc

    def read_file_raw(self, limit:int= 0):
        if self._name.startswith("http"):
            yield from  self.read_url_file(limit, raw=True)
        else:
            yield from self.read_local_file(limit, raw=True)

    def read_file(self, limit:int= 0) -> object:
        if self._name.startswith("http"):
            yield from  self.read_url_file(limit=limit)
        else:
            yield from self.read_local_file(limit=limit)

    def read_url_file(self,
                      limit: int= 0,
                      raw:bool= False)->Iterator[object]:

        # NOTE the stream=True parameter below
        with requests.get(self._name, stream=True) as r:
            r.raise_for_status()
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:  # filter out keep-alive new chunks
                    yield from self.iterate_rows(chunk.splitlines(),limit, raw=raw)

    def read_local_file(self, limit:int=0, raw=False)->Iterator[object]:

        with open(self._name, newline="" ) as csv_file:
            reader = csv.reader(csv_file, delimiter=self._parser.delimiter())
            yield from self.iterate_rows(reader, limit, raw)

