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

    UTF_ENCODING = "utf-8"
    URL_CHUNK_SIZE = 8192

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

        processed_lines = 0

        if self._parser.hasheader():
            next(iterator)

        size = 0
        for processed_lines, row in enumerate(iterator, 1):
            if limit > 0:
                if processed_lines > limit:
                    break
            if raw:
                yield row
            else:
                size = size + len(row)
                print(f"{processed_lines}:size={size}:length={len(row)}:'{row}'")
                doc = self._parser.parse_csv_line(row, processed_lines)
                if self._locator:
                    doc['locator'] = {"f": self._name, "n": processed_lines}
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

    def read_remote(self, limit: int= 0) -> Iterator[str]:

        with requests.get(self._name, stream=True) as r:
            r.raise_for_status()
            for line in r.iter_lines():
                if line:
                    decoded_line = line.decode(FileReader.UTF_ENCODING)
                    yield decoded_line
                # print(f"Chunksize:{len(chunk)}")
                # if chunk:  # filter out keep-alive new chunks
                #     char_block = chunk.decode(FileReader.UTF_ENCODING)
                #     residue = None
                #     for i in char_block.splitlines(keepends=True):
                #         if residue:
                #             yield f"{residue}{i}".rstrip()
                #             residue = None
                #         if i[-1] == "\n":
                #             yield i.rstrip()
                #         else:
                #             residue = i

    def read_url_file(self,
                      limit: int= 0,
                      raw:bool= False)->Iterator[object]:
        reader = csv.reader(self.read_remote(self._name), delimiter=self._parser.delimiter())
        yield from self.iterate_rows(reader,limit, raw=raw)

    def read_local_file(self, limit:int=0, raw=False)->Iterator[object]:

        with open(self._name, newline="" ) as csv_file:
            reader = csv.reader(csv_file, delimiter=self._parser.delimiter())
            yield from self.iterate_rows(reader, limit, raw)

