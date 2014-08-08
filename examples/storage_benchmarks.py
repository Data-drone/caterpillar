# Copyright (c) 2012-2014 Kapiche
# Author: Kris Rogers <kris@kapiche.com>, Ryan Stuart <ryan@kapiche.com>
"""
Print the amount of time in seconds to insert 1000 documents (rows from caterpillar/test_resources/nps_medium.csv)
then dump out profile statistics to storage_benchmarks.profile.

"""
import csv
import os
import shutil
import tempfile
import time

from caterpillar.processing.index import IndexWriter, IndexConfig
from caterpillar.processing.schema import Schema, CATEGORICAL_TEXT, NUMERIC, TEXT
from caterpillar.storage.sqlite import SqliteStorage


with open(os.path.abspath('caterpillar/test_resources/moby.txt'), 'r') as f:
    data = f.read()

path = tempfile.mkdtemp()
start = time.time()
try:
    # Timed run
    with open(os.path.abspath('caterpillar/test_resources/nps_medium.csv'), 'rbU') as f:
        config = IndexConfig(SqliteStorage, Schema(respondant=NUMERIC, region=CATEGORICAL_TEXT(indexed=True),
                                                   store=CATEGORICAL_TEXT(indexed=True),
                                                   liked=TEXT, disliked=TEXT, would_like=TEXT,
                                                   nps=NUMERIC(indexed=True)))
        csv_reader = csv.reader(f)
        csv_reader.next()  # Skip header
        with IndexWriter(os.path.join(path, "timed_index"), config) as writer:
            for row in csv_reader:
                writer.add_document(respondant=row[0], region=row[1], store=row[2], liked=row[3],
                                    disliked=row[4], would_like=row[5], nps=row[6])
    print time.time() - start

    # Profile
    import cProfile
    import pstats

    def profile_index():
        with open(os.path.abspath('caterpillar/test_resources/nps_medium.csv'), 'rbU') as f:
            config = IndexConfig(SqliteStorage, Schema(respondant=NUMERIC, region=CATEGORICAL_TEXT(indexed=True),
                                                       store=CATEGORICAL_TEXT(indexed=True),
                                                       liked=TEXT, disliked=TEXT, would_like=TEXT,
                                                       nps=NUMERIC(indexed=True)))
            csv_reader = csv.reader(f)
            csv_reader.next()  # Skip header
            with IndexWriter(os.path.join(path, "profile_index"), config) as writer:
                for row in csv_reader:
                    writer.add_document(respondant=row[0], region=row[1], store=row[2], liked=row[3],
                                        disliked=row[4], would_like=row[5], nps=row[6])

    pr = cProfile.Profile()
    pr.runcall(profile_index)
    ps = pstats.Stats(pr)
    ps.dump_stats("storage_benchmark.profile")
finally:
    shutil.rmtree(path)
