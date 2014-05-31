# Copyright (c) 2012-2014 Kapiche Limited
# Author: Ryan Stuart <ryan@kapiche.com>
import os
import shutil
import tempfile

from caterpillar.processing.index import find_bi_gram_words, IndexWriter, IndexConfig, IndexReader
from caterpillar.processing.schema import Schema, TEXT
from caterpillar.storage.sqlite import SqliteStorage

path = tempfile.mkdtemp()
try:
    index_dir = os.path.join(path, "example")
    with open('caterpillar/test_resources/alice.txt', 'r') as f:
        data = f.read()
        with IndexWriter(index_dir, IndexConfig(SqliteStorage, Schema(text=TEXT))) as writer:
            writer.add_document(text=data, frame_size=2)
        # What are the bigrams?
        with IndexReader(index_dir) as reader:
            bi_grams = find_bi_gram_words(reader.get_frames())
finally:
    shutil.rmtree(path)

