# Copyright (c) 2012-2014 Kapiche Limited
# Author: Ryan Stuart<ryan@kapiche.com>
"""Create an index then run a filter, printing the number of frames that match the filter."""
import csv
import os
import shutil
import tempfile

from caterpillar.processing.index import IndexWriter, IndexConfig, IndexReader
from caterpillar.processing.schema import Schema, CATEGORICAL_TEXT, NUMERIC, TEXT
from caterpillar.searching.query.match import MatchAllQuery
from caterpillar.searching.query.querystring import QueryStringQuery
from caterpillar.storage.sqlite import SqliteStorage

path = tempfile.mkdtemp()
try:
    index_dir = os.path.join(path, "example")
    with open(os.path.abspath('caterpillar/test_resources/nps_medium.csv'), 'rbU') as f:
        with IndexWriter(index_dir, IndexConfig(SqliteStorage,
                                                Schema(respondant=NUMERIC, region=CATEGORICAL_TEXT(indexed=True),
                                                       store=CATEGORICAL_TEXT(indexed=True), liked=TEXT, disliked=TEXT,
                                                       would_like=TEXT, nps=NUMERIC(indexed=True)))) as writer:
            csv_reader = csv.reader(f)
            csv_reader.next()  # skip header
            for row in csv_reader:
                writer.add_document(respondant=row[0], region=row[1], store=row[2], liked=row[3],
                                    disliked=row[4], would_like=row[5], nps=row[6])

        with IndexReader(index_dir) as reader:
            searcher = reader.searcher()
            print searcher.count(MatchAllQuery([QueryStringQuery("nps >= 8")]))
finally:
    shutil.rmtree(path)

