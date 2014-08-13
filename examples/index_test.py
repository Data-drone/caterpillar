# Copyright (c) 2012-2014 Kapiche Limited
# Author: Ryan Stuart <ryan@kapiche.com>
"""Create an index, store some data."""
import os
import shutil
import tempfile
import nltk
from caterpillar.processing.analysis import stopwords
from caterpillar.processing.analysis.analyse import DefaultAnalyser
from caterpillar.processing.analysis.preprocess import FramePreprocessor

from caterpillar.processing.index import IndexWriter, IndexConfig, IndexReader
from caterpillar.processing.schema import TEXT, Schema, NUMERIC, ID
from caterpillar.storage.sqlite import SqliteStorage

path = tempfile.mkdtemp()
try:
    index_dir = os.path.join(path, "examples")
    with open('caterpillar/test_resources/alice_test_data.txt', 'r') as f:
        data = f.read()
        analyser = DefaultAnalyser(frame_size=2, stopword_list=stopwords.ENGLISH_TEST)
        writer = IndexWriter(index_dir, IndexConfig(SqliteStorage,
                                                    Schema(text=TEXT(analyser=analyser),
                                                           document=TEXT(analyser=analyser, indexed=False),
                                                           blank=NUMERIC(indexed=True), ref=ID(indexed=True))))
        with writer:
            doc_id = writer.add_document(text=data, document='alice.txt', blank=None,
                                         ref=123, frame_size=2)

        with IndexReader(index_dir) as reader:
            assert sum(1 for _ in reader.get_term_positions('nice')) == 3
            assert sum(1 for _ in reader.get_term_positions('key')) == 5

            assert reader.get_term_association('Alice', 'poor') == reader.get_term_association('poor', 'Alice') == 3
            assert reader.get_term_association('key', 'golden') == reader.get_term_association('golden', 'key') == 3

            print """Frame Count: {}
Vocab Size: {}
Number of terms: {}
Alice freq: {}
            """.format(reader.get_frame_count(), reader.get_vocab_size(), sum(1 for _ in reader.get_frequencies()), reader.get_term_frequency('Alice'))
            # assert reader.get_vocab_size() == sum(1 for _ in reader.get_frequencies()) == 504
            # assert reader.get_term_frequency('Alice') == 23
            # frames = [frame for fid, frame in reader.get_frames()]
            # frames.sort(key=lambda x: int(x['_sequence_number']))
            # for frame in frames:
            #     print '{}-----'.format(frame['_sequence_number'])
            #     print frame['_text']
            frames = dict(reader.get_frames()).values()
            frames.sort(key=lambda f: f['_sequence_number'])
            count = 0
            for f in frames:
                count += 1
                print '{}----'.format(f['_sequence_number'])
                print f['_text']
            print count
finally:
    shutil.rmtree(path)
