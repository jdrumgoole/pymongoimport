"""
Created on 23 Jul 2017

@author: jdrumgoole


"""
import time
from datetime import datetime
import os
import logging
import stat

import pymongo
from pymongo import errors

from pymongoimport.filereader import FileReader
from pymongoimport.linetodictparser import LineToDictParser



class FileWriter(object):

    def __init__(self,
                 doc_collection : pymongo.collection,
                 reader: FileReader,
                 parser: LineToDictParser,
                 audit_collection : pymongo.collection =None,
                 batch_size: int = 1000):

        self._logger = logging.getLogger(__name__)
        self._collection = doc_collection
        self._audit_collection = audit_collection
        self._batch_size = batch_size
        self._totalWritten = 0
        self._reader = reader
        self._parser = parser
        #
        # Need to work out stat manipulation for mongodb insertion
        #

        if self._audit_collection:
            self._audit_collection.insert_one({"filestamp": self._reader.name,
                                               "timestamp": datetime.utcnow()})

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
            dummy = f.readline()  # skipcount may be bigger than the number of lines i  the file
            while dummy:
                line_count = line_count + 1
                if line_count == skip_count:
                    break
                dummy = f.readline()
        return line_count

    def write(self, limit=0, restart=False):

        total_written = 0
        time_start = time.time()
        inserted_this_quantum = 0
        total_read = 0
        insert_list = []
        try:
            interval_timer = time_start
            for i, line in enumerate(self._reader.readline(limit=limit), 1):
                insert_list.append(self._parser.parse_list(line, i))
                if len(insert_list) % self._batch_size == 0:
                    results = self._collection.insert_many(insert_list)
                    total_written = total_written + len(results.inserted_ids)
                    inserted_this_quantum = inserted_this_quantum + len(results.inserted_ids)

                    time_now = time.time()
                    elapsed = time_now - interval_timer
                    docs_per_second = len(insert_list) / elapsed
                    interval_timer = time_now
                    insert_list = []
                    self._logger.info(
                            f"Input:'{self._reader.name}': docs per sec:{docs_per_second:7.0f}, total docs:{total_written:>10}")

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

        time_finish = time.time()
        #self._logger.info("Total elapsed time to upload '%s' : %s", self._reader.name, seconds_to_duration(finish - time_start))
        #self._logger.info(f"Total elapsed time to upload '{self._reader.name}' : {seconds_to_duration(time_finish - time_start)}")

        return total_written, time_finish - time_start
