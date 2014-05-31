# Copyright (c) 2012-2014 Kapiche Limited
# Author: Kris Rogers <kris@kapiche.com>, Ryan Stuart<ryan@kapiche.com>
import os
import shutil
import tempfile
from caterpillar.processing.index import IndexWriter, IndexConfig, IndexReader

from caterpillar.processing.schema import TEXT, Schema
from caterpillar.searching.query.querystring import QueryStringQuery
from caterpillar.storage.sqlite import SqliteStorage

path = tempfile.mkdtemp()
try:
    index_dir = os.path.join(path + "examples")
    with open('caterpillar/test_resources/alice.txt', 'r') as f:
        data = f.read()
        with IndexWriter(index_dir, IndexConfig(SqliteStorage, Schema(text=TEXT))) as writer:
            writer.add_document(fold_case=True, text=data)

        with IndexReader(index_dir) as reader:
            searcher = reader.searcher()
            results = searcher.search(QueryStringQuery('W*e R?bbit and (thought or little^1.5)'))
            print "Retrieved {} of {} matches".format(len(results), results.num_matches)
finally:
    shutil.rmtree(path)
