# Copyright (c) 2012-2014 Kapiche Limited
# Author: Kris Rogers <kris@kapiche.com>, Ryan Stuart <ryan@kapiche.com>
import csv
import os
import shutil
import tempfile

from caterpillar.processing.index import IndexConfig, IndexWriter
from caterpillar.processing.schema import TEXT, Schema
from caterpillar.storage.sqlite import SqliteStorage


path = tempfile.mkdtemp()
try:
    # Big frames
    index_dir = os.path.join(path, "big_frames")
    with open('caterpillar/test_resources/alice.txt', 'r') as f:
        data = f.read()
        with IndexWriter(index_dir, IndexConfig(SqliteStorage, Schema(text=TEXT))) as writer:
            writer.add_document(frame_size=0, text=data)

    # Lots of frames, big vocabulary
    index_dir = os.path.join(path, "memory")
    with IndexWriter(index_dir, IndexConfig(SqliteStorage, Schema(text=TEXT))) as writer:
        with open('caterpillar/test_resources/twitter_sentiment.csv', 'r') as f:
            csv_reader = csv.reader(f)
            csv_reader.next()  # Skip header
            for row in csv_reader:
                writer.add_document(text=row[1])
        with open('caterpillar/test_resources/promoters.csv', 'r') as f:
            csv_reader = csv.reader(f)
            csv_reader.next()  # Skip header
            for row in csv_reader:
                writer.add_document(text=row[0])
        with open('caterpillar/test_resources/detractors.csv', 'r') as f:
            csv_reader = csv.reader(f)
            csv_reader.next()  # Skip header
            for row in csv_reader:
                writer.add_document(text=row[0])
        with open('caterpillar/test_resources/moby.txt', 'r') as f:
            data = f.read()
            writer.add_document(text=data)
        with open('caterpillar/test_resources/alice.txt', 'r') as f:
            data = f.read()
            writer.add_document(text=data)
        writer.fold_term_case()
finally:
    shutil.rmtree(path)

