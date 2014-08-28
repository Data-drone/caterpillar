# Copyright (c) 2012-2014 Kapiche Limited
# Author: Ryan Stuart <ryan@kapiche.com>
"""Merge indexes together."""
from itertools import izip_longest, ifilter
import logging

import begin
from concurrent.futures import ProcessPoolExecutor
from caterpillar.processing.index import IndexReader


def work(indexes):
    docs = 0
    terms = set()
    readers = [IndexReader(index) for index in ifilter(lambda x: x, indexes)]

    for reader in readers:
        reader.begin()

    # def merge_term(terms):
    #     merge = {k: dict() for k in terms}
    #     for reader in readers:
    #         for term in reader.get_positions_index()

    for reader in readers:
        terms.union(set([k for k, c in reader.get_frequencies()]))
        docs += reader.get_document_count()

    # Don't forget to close our readers!
    for reader in readers:
        reader.close()

    return docs


@begin.start
@begin.convert(_automatic=True)
def run(step_size=10, *indexes):
    """
    Merge all ``indexes`` together into a single index written into ``output_dir``.

    """
    print len(indexes)
    args = [iter(indexes)] * step_size
    documents = 0
    pool = ProcessPoolExecutor()

    try:
        for docs in pool.map(work, izip_longest(*args)):
            documents += docs
        print "{:,} documents".format(documents)
    finally:
        pool.shutdown()
