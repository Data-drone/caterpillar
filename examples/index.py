# Copyright (c) 2012-2014 Kapiche Limited
# Author: Ryan Stuart <ryan@kapiche.com>
"""Create an index, store some data."""
import os
import shutil
import tempfile

from caterpillar.processing.index import IndexWriter, IndexConfig
from caterpillar.processing.schema import TEXT, Schema, NUMERIC
from caterpillar.storage.sqlite import SqliteStorage

path = tempfile.mkdtemp()
try:
    index_dir = os.path.join(path, "examples")
    with open('caterpillar/test_resources/moby.txt', 'r') as f:
        data = f.read()
        with IndexWriter(index_dir, IndexConfig(SqliteStorage, Schema(text=TEXT, some_number=NUMERIC))) as writer:
            writer.add_document(text=data, some_number=1)
finally:
    shutil.rmtree(path)
