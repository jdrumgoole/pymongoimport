"""
Created on 23 Jul 2017

@author: jdrumgoole


"""
import time
from datetime import datetime, timedelta
import requests
import logging
import pymongo
from pymongo import errors

from pymongoimport.filereader import FileReader

def seconds_to_duration(seconds):
    delta = timedelta(seconds=seconds)
    d = datetime(1, 1, 1) + delta
    return "%02d:%02d:%02d:%02d" % (d.day - 1, d.hour, d.minute, d.second)


class FileWriter(object):

    def __init__(self, collection : pymongo.collection,
                 reader: FileReader,
                 batch_size: int = 1000):

        self._logger = logging.getLogger(__name__)
        self._collection = collection
        self._batch_size = batch_size
        self._totalWritten = 0
        self._reader = reader

    @property
    def batch_size(self):
        return self._batch_size

    @batch_size.setter
    def batch_size(self, size: int) -> None:
        if size < 1:
            raise ValueError(f"Invalid batchsize: {size}")
        self._batch_size = size

    @staticmethod
    def skipLines(f, skip_count:int):
        """
        >>> f = open( "test_set_small.txt", "r" )
        >>> skipLines( f , 20 )
        20
        """

        line_count = 0
        if skip_count > 0:
            # print( "Skipping")
            dummy = f.readline()  # skicaount may be bigger than the number of lines i  the file
            while dummy:
                line_count = line_count + 1
                if line_count == skip_count:
                    break
                dummy = f.readline()
        return line_count

    def write(self, restart=False):

        start = time.time()
        total_written = 0
        results = None

        docs_generator = self._reader.read_file()

        time_start = time.time()
        inserted_this_quantum = 0
        total_read = 0
        insert_list = []

        try:
            for doc in docs_generator:
                insert_list.append(doc)
                if total_read % self._batch_size == 0:
                    results = self._collection.insert_many(insert_list)
                    total_written = total_written + len(results.inserted_ids)
                    inserted_this_quantum = inserted_this_quantum + len(results.inserted_ids)
                    insert_list = []
                    time_now = time.time()
                    elapsed = time_now - time_start
                    docs_per_second = self._batch_size / elapsed
                    time_start = time_now
                    self._logger.info(
                            f"Input:'{self._reader.name}': docs per sec:{docs_per_second:7.0f}, \
                            total docs:{total_written:>10}")

        except UnicodeDecodeError as exp:
            if self._logger:
                self._logger.error(exp)
                self._logger.error("Error on line:%i", total_read + 1)
            raise;

        if len(insert_list) > 0:
            # print(insert_list)
            try:
                results = self._collection.insert_many(insert_list)
                total_written = total_written + len(results.inserted_ids)
                self._logger.info("Input: '%s' : Inserted %i records", self._reader.name, total_written)
            except errors.BulkWriteError as e:
                self._logger.error(f"pymongo.errors.BulkWriteError: {e.details}")

        finish = time.time()
        self._logger.info("Total elapsed time to upload '%s' : %s", self._reader.name, seconds_to_duration(finish - start))
        return total_written
