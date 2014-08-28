# Copyright (c) 2012-2014 Kapiche Limited
# Author: Ryan Stuart <ryan@kapiche.com>
"""Merge indexes together."""
import logging

import begin
from concurrent.futures import ProcessPoolExecutor
from caterpillar.processing.index import IndexReader


def work(index):
    vocab_size = set()
    total_term_freq = 0
    with IndexReader(index) as reader:
        for term, count in reader.get_frequencies():
            vocab_size.add(term)
            total_term_freq += count
        return vocab_size, total_term_freq, reader.get_document_count()


@begin.start
def run(*indexes):
    """Report combined statistics about ``indexes``."""
    vocab_size = set()
    total_term_freq = 0
    documents = 0
    num_indexes = len(indexes)
    pool = ProcessPoolExecutor()
    try:
        for vocab, freq, doc_count in pool.map(work, indexes):
            vocab_size.union(vocab)
            total_term_freq += freq
            documents += doc_count
        print "{:,} indexes; {:,} terms; {:,} term freq; {:,} docs.".\
            format(num_indexes, len(vocab_size), total_term_freq, documents)
    finally:
        pool.shutdown()
