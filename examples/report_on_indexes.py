# Copyright (c) 2012-2014 Kapiche Limited
# Author: Ryan Stuart <ryan@kapiche.com>
"""Merge indexes together."""
import logging

import begin
from concurrent.futures import ProcessPoolExecutor
from caterpillar.processing.index import IndexReader


def work(index):
    with IndexReader(index) as reader:
        freqs = reader.get_frequencies()
        vocab_size = freqs.viewkeys()
        total_term_freq = sum(freqs.values())
        docs = reader.get_document_count()
    return vocab_size, total_term_freq, docs


@begin.start
def run(*indexes):
    """Report combined statistics about ``indexes``."""
    vocab_size = set()
    total_term_freq = 0
    documents = 0
    num_indexes = len(indexes[:2])
    # pool = ProcessPoolExecutor()
    # try:
    for vocab, freq, doc_count in map(work, indexes[:2]):
        vocab_size.update(vocab)
        total_term_freq += freq
        documents += doc_count
    print "{:,} indexes; {:,} unique terms; {:,} total terms; {:,} docs.".\
        format(num_indexes, len(vocab_size), total_term_freq, documents)
    # finally:
    #     pool.shutdown()
