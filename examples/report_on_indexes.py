# Copyright (c) 2012-2014 Kapiche Limited
# Author: Ryan Stuart <ryan@kapiche.com>
"""Merge indexes together."""
import uuid
import begin
from concurrent.futures import ProcessPoolExecutor
import struct
from caterpillar.processing.index import IndexReader


def work(index):
    with IndexReader(index) as reader:
        positions = reader.get_positions_index()
        freq = 0
        for term, data in positions.iteritems():
            assert not len(data) % 20
            freq += len(data) / 20
        vocab_size = positions.keys()
        docs = reader.get_document_count()
        # Example of reading a line from positions and a document from that line.
        # a_record = struct.unpack('>16sl', positions[vocab_size[0]][:20])  # tuple (doc_id_uuid_bytes, freq)
        # doc_id = uuid.UUID(bytes=a_record[0]).hex
        # print doc_id, a_record[1], reader.get_document(doc_id)
    return vocab_size, freq, docs


@begin.start
def run(*indexes):
    """Report combined statistics about ``indexes``."""
    vocab_size = set()
    total_term_freq = 0
    documents = 0
    num_indexes = len(indexes)
    pool = ProcessPoolExecutor()

    try:
    # for vocab, freq, doc_count in map(work, indexes[:2]):
        for vocab, freq, doc_count in pool.map(work, indexes):
            vocab_size.update(vocab)
            total_term_freq += freq
            documents += doc_count
        print "{:,} indexes; {:,} unique terms; {:,} total terms; {:,} docs.".\
            format(num_indexes, len(vocab_size), total_term_freq, documents)
    finally:
        pool.shutdown()
