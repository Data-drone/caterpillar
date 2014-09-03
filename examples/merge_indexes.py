# Copyright (c) 2012-2014 Kapiche Limited
# Author: Ryan Stuart <ryan@kapiche.com>
"""Merge indexes together."""
from itertools import izip_longest, ifilter
import logging
import os
import random
import ujson as json

import begin
from concurrent import futures
import time
from caterpillar.processing.index import IndexReader, IndexWriter, DuplicateIndexError


def merge_indexes(path, sub_index_paths):
    start = time.time()
    logging.info("Merging {:,} indexes into {}".format(len(sub_index_paths), path))
    path = os.path.join(path, "{}".format(random.SystemRandom().randint(0, 10**5)))
    if os.path.exists(path):
        raise DuplicateIndexError('Index already exists at {}'.format(path))
    readers = [IndexReader(index) for index in ifilter(lambda x: x, sub_index_paths)]
    config = readers[0].config
    schema = config.schema

    # Create the index storage
    os.makedirs(path)
    with open(os.path.join(path, IndexWriter.CONFIG_FILE), 'w') as f:
        f.write(config.dumps())
    # Initialize storage
    storage = config.storage_cls(path, create=True)
    # Need to create the containers
    storage.begin()
    storage.add_container(IndexWriter.DOCUMENTS_CONTAINER)
    storage.add_container(IndexWriter.FRAMES_CONTAINER)
    storage.add_container(IndexWriter.SETTINGS_CONTAINER)
    storage.add_container(IndexWriter.INFO_CONTAINER)
    storage.add_container(IndexWriter.POSITIONS_CONTAINER)
    storage.add_container(IndexWriter.ASSOCIATIONS_CONTAINER)
    storage.add_container(IndexWriter.FREQUENCIES_CONTAINER)
    storage.add_container(IndexWriter.METADATA_CONTAINER)
    # Revision
    storage.set_container_item(IndexWriter.INFO_CONTAINER, 'revision',
                               json.dumps(random.SystemRandom().randint(0, 10**10)))
    storage.commit()

    # Get ready to write
    storage = config.storage_cls(path, create=False)
    storage.begin()

    # Start the readers
    for reader in readers:
        reader.begin()

    # Merge the documents and frames all in 1 go. Also record terms to merge.
    logging.info("Merging frames and docs, recording terms...")
    docs = 0
    frames = 0
    terms = set()
    for reader in readers:
        docs += reader.get_document_count()
        frames += reader.get_frame_count()
        storage.set_container_items(IndexWriter.DOCUMENTS_CONTAINER,
                                    reader.storage.get_container_items(IndexWriter.DOCUMENTS_CONTAINER))
        storage.set_container_items(IndexWriter.FRAMES_CONTAINER,
                                    reader.storage.get_container_items(IndexWriter.FRAMES_CONTAINER))
        terms.update(reader.get_terms())
    logging.info("Merged {:,} documents and {:,} frames, recorded {:,} terms in {:,}s.".
                 format(docs, frames, len(terms), time.time() - start))

    # Merge terms, 1000 at a time to save memory
    logging.info("Merging positions...")
    terms = list(terms)
    for term_chunk in [terms[i: i+500000] for i in xrange(0, len(terms), 500000)]:
        merge = dict.fromkeys(ifilter(lambda t: t, term_chunk), '')
        for reader in readers:
            for term, pos in reader.get_positions_index(keys=term_chunk).iteritems():
                merge[term] += pos[:]
        storage.set_container_items(IndexWriter.POSITIONS_CONTAINER, merge)
    logging.info("Merged positions.")

    # Don't forget to close our readers and storage!
    for reader in readers:
        reader.close()
    storage.commit()
    storage.close()
    logging.info("Finished merging into {}, took {:,}s.".format(path, time.time()-start))


@begin.start
@begin.convert(_automatic=True)
def run(out_dir, log_lvl="INFO", step_size=10, *indexes):
    """Merge all ``indexes`` together into a single index written into ``output_dir``."""
    logging.basicConfig(level=log_lvl, format='%(asctime)s - %(levelname)s - %(processName)s: %(message)s')
    start = time.time()
    count = 0
    # indexes = indexes[:10]
    pool = futures.ProcessPoolExecutor()

    try:
    # for _ in map(merge_indexes, [out_dir for _ in xrange(0, len(indexes), step_size)],
    #              [indexes[i:i+step_size] for i in xrange(0, len(indexes), step_size)]):
        for _ in pool.map(merge_indexes, [out_dir for _ in xrange(0, len(indexes), step_size)],
                          [indexes[i:i+step_size] for i in xrange(0, len(indexes), step_size)]):
            count += 1
            if not count % 10:
                print "Processed {:,} indexes...".format(count*10)
        print "Processed all indexes in {:,.02f} seconds.".format(time.time()-start)
    finally:
        pool.shutdown()
