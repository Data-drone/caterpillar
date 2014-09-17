# Copyright (c) 2012-2014 Kapiche Limited
# Aurthor: Ryan Stuart <ryan@kapiche.com>
"""
Read in a bunch of pre-parsed csv files produced from the Wikipedia XML dump (1 article per row) and turn them into an
index.

"""
from collections import namedtuple
import csv
import logging
import os
import sys

import begin
from concurrent import futures
import time

from caterpillar.processing.index import IndexWriter, IndexConfig, IndexReader
from caterpillar.processing.schema import Schema, ID, TEXT, CATEGORICAL_TEXT
from caterpillar.storage.sqlite import SqliteStorage


def index(f, index):
    config = IndexConfig(SqliteStorage, Schema(title=TEXT(indexed=False, stored=True),
                                               text=TEXT(indexed=True, stored=False),
                                               snippet=CATEGORICAL_TEXT(stored=True),
                                               url=CATEGORICAL_TEXT(stored=True)))
    Page = namedtuple('Page', 'p_id, title, text')
    with open(f, 'rU') as csf_file:
        pid = os.getpid()
        reader = csv.reader(csf_file)
        count = 0
        real_count = 0
        index_name = 1
        index_count = 0
        logging.info("Hi, I'm worker {} here to do your dirty work with {}.".format(pid, f))
        writer = IndexWriter("{}-{}-{}".format(index, index_name, index_name+999), config)
        writer.begin()
        for page in map(Page._make, reader):
            real_count += 1
            if not writer:
                writer = IndexWriter("{}-{}-{}".format(index, index_name, index_name+999), config)
                writer.begin()
            count += 1
            writer.add_document(title=page[1], text=page[2], snippet=page[2].decode('utf-8')[:256],
                                url=u"http://en.wikipedia.com/wiki/{}".format(page[1].decode('utf-8').replace(" ", "_")))
            if count % 1000 == 0:
                logging.info("Parsed {:,} documents so far, {:,} actual articles, writing out this "
                              "index ({})...".
                              format(real_count, count, "{}-{}-{}".format(index, index_name, index_name+1000)))
                writer.commit()
                writer.close()
                writer = None
                index_name += 1000
                index_count += 1
            if real_count % 1000 == 0:
                logging.info("Parsed {:,} documents so far, {:,} actual articles, {:,} indexes, "
                              "continuing...".format(real_count, count, index_count))
        if writer:
            logging.info("Parsed all {:,} documents, {:,} actual articles, writing final index ({})...".
                         format(real_count, count, "{}-{}-{}".format(index, index_name, index_name+999)))
            writer.commit()
            writer.close()
            index_count += 1
        else:
            logging.info("Parsed all {:,} documents, {:,} actual articles...".
                         format(real_count, count))
        logging.info("Finished file {}. Created {:,} indexes.".format(f, index_count+1))
        return index_count

@begin.start
@begin.convert(profile=bool)
def run(index_path="/tmp/wiki-index", profile_dump=False, log_lvl='INFO', *files):
    """Parse the Wikipedia csv dumps in ``files`` and save in an index per csv file at ``index_path``."""
    logging.basicConfig(level=log_lvl, format='%(asctime)s - %(levelname)s - %(processName)s: %(message)s')
    logging.info("Adding all documents from the {:,} Wikipedia csv dumps...Wew!".format(len(files)))
    csv.field_size_limit(100000000000000)

    now = time.time()
    paths = [os.path.join(index_path, os.path.basename(f)[:-4]) for f in files]
    indexes = 0
    # with futures.ProcessPoolExecutor() as pool:
        # for index_count in pool.map(index, files, paths):
    for index_count in map(index, files, paths):
        indexes += index_count
    logging.info("All done. We processed {:,} files in {:,.02f} seconds and created {:,} indexes.".\
        format(len(files), time.time()-now, indexes))

