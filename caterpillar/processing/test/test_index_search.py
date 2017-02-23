# Copyright (c) 2012-2014 Kapiche Limited
# Author: Sam Hames <sam.hames@kapiche.com>
"""Test search functionality:

1. Low level filter and filter_and_rank search primitives.
2. Integration of the resultset functionality with search output.

"""
from __future__ import division

import csv
import os
import pytest

from caterpillar.storage.sqlite import SqliteStorage
from caterpillar.processing.analysis.analyse import EverythingAnalyser
from caterpillar.processing.index import IndexWriter, IndexReader, IndexConfig

from caterpillar.processing import schema
from caterpillar.test_util import TestAnalyser


"""
Test plan combos

filter | search
metadata only | search with metadata
all of the field types for metadata
frames | documents
keyset pagination | no pagination
limits | no limits
scoring | no scoring
include fields | exclude fields
empty resultsets | empty termsets | no search functions

"""


def test_searching_filtering_nps(index_dir):
    """Test searching nps-backed data."""
    with open('caterpillar/test_resources/big.csv', 'rbU') as f:
        analyser = TestAnalyser()
        config = IndexConfig(
            SqliteStorage, schema.Schema(
                respondant=schema.NUMERIC,
                region=schema.CATEGORICAL_TEXT(indexed=True),
                store=schema.CATEGORICAL_TEXT(indexed=True),
                liked=schema.TEXT(analyser=analyser),
                disliked=schema.TEXT(analyser=analyser),
                would_like=schema.TEXT(analyser=analyser),
                nps=schema.NUMERIC(indexed=True),
                fake=schema.NUMERIC(indexed=True),
                fake2=schema.CATEGORICAL_TEXT(indexed=True),
                fake3=schema.CATEGORICAL_TEXT(indexed=True)
            )
        )
        with IndexWriter(index_dir, config) as writer:
            csv_reader = csv.reader(f)
            csv_reader.next()  # Skip header
            empty_rows = 0
            for row in csv_reader:
                if len(row[3]) + len(row[4]) + len(row[5]) == 0:
                    empty_rows += 1
                writer.add_document(respondant=row[0], region=row[1], store=row[2], liked=row[3],
                                    disliked=row[4], would_like=row[5], nps=row[6], fake2=None, fake3=' spaces ')

        with IndexReader(index_dir) as reader:

            results = reader.filter(should=['point', 'pointed', 'points'], include_fields=['would_like'])
            assert len(results) == 14

            # Verify uniqueness of returned results
            docs = set()
            result_count = 0
            for field in ['liked', 'disliked', 'would_like']:
                # query returns the same 5 documents for each unstructured field searched.
                results = reader.filter(metadata={'region': {'=': 'Otago'}, 'nps': {'<': 5}}, include_fields=[field])
                result_count += len(results)
                for frame_id, frame in reader.get_frames(frame_ids=results, field=None):
                    docs.add(frame['_doc_id'])

            # Individual field searches should match the overall field searches.
            assert len(docs) == 5 == len(
                reader.filter(
                    metadata={'region': {'=': 'Otago'}, 'nps': {'<': 5}}, return_documents=True
                )
            )
            assert result_count == 15 == len(
                reader.filter(
                    metadata={'region': {'=': 'Otago'}, 'nps': {'<': 5}}
                )
            )

            # Metadata field searching
            metadata = {
                'nps': {'=': 10},
                'store': {'=': 'DANNEVIRKE'}
            }
            assert len(reader.filter(metadata=metadata)) == 6 == \
                len(reader.filter(metadata=metadata, include_fields=['liked'])) + \
                len(reader.filter(metadata=metadata, include_fields=['disliked'])) + \
                len(reader.filter(metadata=metadata, include_fields=['would_like']))

            num_christchurch = len(reader.filter(metadata={'region': {'=': 'Christchurch'}}, include_fields=['liked']))
            valid_nps_christchurch = len(
                reader.filter(metadata={'region': {'=': 'Christchurch'}, 'nps': {'>': 0}}, include_fields=['liked'])
            )
            assert valid_nps_christchurch < num_christchurch

            # Test invariant properties of sets for comparing a subset of terms.
            all_terms = {term: freq for i, (term, freq) in enumerate(reader.get_frequencies('liked')) if i % 40 == 0}

            for left_term in all_terms:
                for right_term in all_terms:

                    if left_term == right_term:
                        continue

                    left_or_right = len(reader.filter(should=[left_term, right_term], include_fields=['liked']))
                    left_and_right = len(reader.filter(must=[left_term, right_term], include_fields=['liked']))
                    left_not_right = len(reader.filter(
                        should=[left_term], must_not=[right_term], include_fields=['liked'])
                    )
                    right_not_left = len(reader.filter(
                        must_not=[left_term], should=[right_term], include_fields=['liked'])
                    )

                    assert left_or_right == (left_and_right + left_not_right + right_not_left)
                    assert left_and_right <= min(all_terms[left_term], all_terms[right_term])

            for i in range(1, 11):
                total = len(reader.filter(metadata={'nps': {'>': 0}}))
                assert (
                    len(reader.filter(metadata={'nps': {'>=': i}})) +
                    len(reader.filter(metadata={'nps': {'<': i}})) ==
                    total
                )

            assert len(reader.filter(metadata={'fake': {'=': 1}})) == 0
            assert len(reader.filter(metadata={'region': {'=': 'asdfjhsdfsdfa'}})) == 0
            assert (
                len(reader.filter(metadata={'fake3': {'=': ' spaces '}}, include_fields=['liked'])) ==
                reader.get_frame_count('liked')
            )

            # Check incorrect usage of various search things.


def test_search_composition(index_dir):
    with open(os.path.abspath('caterpillar/test_resources/alice_test_data.txt'), 'r') as f:
        data = f.read()
        analyser = TestAnalyser()

        writer = IndexWriter(
            index_dir, IndexConfig(
                SqliteStorage,
                schema.Schema(
                    text1=schema.TEXT(analyser=analyser),
                    text2=schema.TEXT(analyser=analyser),
                    document=schema.TEXT(analyser=analyser, indexed=False),
                    flag=schema.FieldType(analyser=EverythingAnalyser(), indexed=True, categorical=True)
                )
            )
        )

        # Add the document in one field
        with writer:
            writer.add_document(text1=data, document='alice.txt', flag=True, frame_size=2)

        # Test all/single field retrieval

        # Add the document in the other field and compare

        # Add the document again to both fields - count documents and frames.
