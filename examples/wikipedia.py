# Copyright (c) 2012-2014 Kapiche Limited
# Aurthor: Ryan Stuart <ryan@kapiche.com>
"""
Read in a bunch of pre-parsed csv files produced from the Wikipedia XML dump (1 article per row) and turn them into an
index.

"""
import cProfile
from collections import namedtuple
import csv
import os
import pstats
import sys

import begin
from concurrent import futures
from memory_profiler import profile
import time

from caterpillar.processing.index import IndexWriter, IndexConfig, IndexReader
from caterpillar.processing.schema import Schema, ID, TEXT
from caterpillar.storage.sqlite import SqliteStorage


def index(f, index):
    config = IndexConfig(SqliteStorage, Schema(id=ID, title=TEXT, text=TEXT))
    Page = namedtuple('Page', 'p_id, title, redirect, revision, text')
    with open(f, 'rU') as csf_file:
        pid = os.getpid()
        reader = csv.reader(csf_file)
        count = 0
        real_count = 0
        size = 0
        index_name = 1
        index_count = 0
        print "Hi, I'm worker {} here to do your dirty work with {}.".format(pid, f)
        writer = IndexWriter("{}-{}-{}".format(index, index_name, index_name+999), config)
        writer.begin()
        for page in map(Page._make, reader):
            real_count += 1
            if not int(page[2]):
                size += sys.getsizeof(page[4])
                count += 1
                writer.add_document(id=page[0], title=page[1], text=page[4])
                if count % 1000 == 0:
                    sys.stdout.write("{}: Parsed {:,} documents so far, {:,} actual articles, {:,} bytes, writing "
                                     "out this index ({})...".format(f, real_count, count, size,
                                                                     "{}-{}-{}".format(index, index_name, index_name+1000)))
                    sys.stdout.flush()
                    writer.commit()
                    writer.close()
                    sys.stdout.write("DONE\n")
                    index_name += 1000
                    size = 0
                    index_count += 1
                    writer = IndexWriter("{}-{}-{}".format(index, index_name, index_name+999), config)
                    writer.begin()
            if real_count % 1000 == 0:
                print "{}: Parsed {:,} documents so far, {:,} actual articles, {:,} bytes, {:,}, indexes, " \
                      "continuing...".format(f, real_count, count, size, index_count)
        sys.stdout.write("{}: Parsed all {:,} documents, {:,} actual articles, {:,} bytes, writing final index "
                         "({})...". format(f, real_count, count, size,
                                           "{}-{}-{}".format(index, index_name, index_name+999)))
        sys.stdout.flush()
        writer.commit()
        writer.close()
        sys.stdout.write("DONE\n")
        print "We created {:,} indexes.".format(index_count+1)

@begin.start
@begin.convert(num_of_docs=int, step_size=int, profile=bool)
def run(index_path="/tmp/wiki-index", profile_dump=False, *files):
    """
    Parse the Wikipedia csv dumps in ``files`` and save in an index per csv file at ``index_path``.

    """
    print "Adding all documents from the {:,} Wikipedia csv dumps...Wew!".format(len(files))
    csv.field_size_limit(100000000000000)


    # def work():
    now = time.time()
    paths = [os.path.join(index_path, os.path.basename(f)[:-4]) for f in files]
    pool = futures.ProcessPoolExecutor()
    futures.wait(pool.map(index, files, paths))
    print "All done. We processed {:,} files in {:,.02f} seconds.".format(len(files), time.time()-now)

    # if profile_dump:
    #     pr = cProfile.Profile()
    #     pr.runcall(work)
    #     ps = pstats.Stats(pr)
    #     ps.dump_stats("storage_benchmark.profile")
    # else:
    # work()



