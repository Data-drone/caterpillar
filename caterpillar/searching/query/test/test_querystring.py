# Copyright (c) 2012-2014 Kapiche
# Author: Kris Rogers <kris@kapiche.com>
"""Tests for the querystring module."""
import os
from caterpillar.storage.sqlite import SqliteStorage

import pytest

from caterpillar.processing import schema
from caterpillar.processing.index import IndexWriter, IndexReader, IndexConfig
from caterpillar.searching.query.querystring import QueryStringQuery
from caterpillar.searching.query import QueryError


def test_querystring_query_basic(index_dir):
    """Test querystring query basic functionality."""
    with open(os.path.abspath('caterpillar/test_resources/alice.txt'), 'rbU') as f:
        data = f.read()
        with IndexWriter(index_dir, IndexConfig(SqliteStorage, schema=schema.Schema(text=schema.TEXT))) as writer:
            writer.add_document(text=data)

    # Simple terms
    with IndexReader(index_dir) as reader:
        alice_count = len(QueryStringQuery('Alice', 'text').evaluate(reader).frame_ids['text'])
        king_count = len(QueryStringQuery('King', 'text').evaluate(reader).frame_ids['text'])
        assert alice_count > 0
        assert king_count > 0
        # Boolean operators
        alice_and_king_count = len(QueryStringQuery('Alice AND King', 'text').evaluate(reader).frame_ids['text'])
        alice_not_king_count = len(QueryStringQuery('Alice NOT King', 'text').evaluate(reader).frame_ids['text'])
        alice_or_king_count = len(QueryStringQuery('Alice OR King', 'text').evaluate(reader).frame_ids['text'])
        king_not_alice_count = len(QueryStringQuery('King NOT Alice', 'text').evaluate(reader).frame_ids['text'])
        assert alice_not_king_count == alice_count - alice_and_king_count
        assert king_not_alice_count == king_count - alice_and_king_count
        assert alice_or_king_count == alice_not_king_count + king_not_alice_count + alice_and_king_count
        # Wildcards
        assert len(QueryStringQuery('*ice', 'text').evaluate(reader).frame_ids['text']) > alice_count
        assert len(QueryStringQuery('K??g', 'text').evaluate(reader).frame_ids['text']) == king_count

        with pytest.raises(QueryError):
            QueryStringQuery('Alice', 'not_a_field').evaluate(reader)


def test_querystring_query_advanced(index_dir):
    """Test querysting query advanced searching."""
    config = IndexConfig(SqliteStorage, schema.Schema(liked=schema.TEXT, disliked=schema.TEXT,
                                                      age=schema.NUMERIC(indexed=True),
                                                      gender=schema.CATEGORICAL_TEXT(indexed=True)))
    with IndexWriter(index_dir, config) as writer:
        writer.add_document(liked='product', disliked='service', age=20, gender='male')
        writer.add_document(liked='service', disliked='product', age=30, gender='male')
        writer.add_document(liked='service', disliked='price', age=40, gender='female')
        writer.add_document(liked='product', disliked='product', age=80, gender='female')

    # Metadata
    with IndexReader(index_dir) as reader:
        # Test presence of terms for each text_field
        # field: "liked":
        assert len(QueryStringQuery('age=80', 'liked').evaluate(reader).frame_ids['liked']) == 1
        assert len(QueryStringQuery('age<80', 'liked').evaluate(reader).frame_ids['liked']) == 3
        assert len(QueryStringQuery('age>=20', 'liked').evaluate(reader).frame_ids['liked']) == 4
        assert len(QueryStringQuery('product not gender=male', 'liked').evaluate(reader).frame_ids['liked']) == 1
        assert len(QueryStringQuery('product not gender=*male', 'liked').evaluate(reader).frame_ids['liked']) == 0
        assert 'disliked' not in QueryStringQuery('age=80', 'liked').evaluate(reader).frame_ids

        # field: "disliked":
        assert len(QueryStringQuery('age=80', 'disliked').evaluate(reader).frame_ids['disliked']) == 1
        assert len(QueryStringQuery('age<80', 'disliked').evaluate(reader).frame_ids['disliked']) == 3
        assert len(QueryStringQuery('age>=20', 'disliked').evaluate(reader).frame_ids['disliked']) == 4
        assert len(QueryStringQuery('product not gender=male', 'disliked').evaluate(reader).frame_ids['disliked']) == 1
        assert len(QueryStringQuery('product not gender=*male', 'disliked').evaluate(reader).frame_ids['disliked']) == 0
        assert 'liked' not in QueryStringQuery('age=80', 'disliked').evaluate(reader).frame_ids

        # Extras
        assert len(QueryStringQuery('product', 'liked').evaluate(reader).frame_ids['liked']) == 2
        assert len(QueryStringQuery('gender=female not product',
                                    'disliked').evaluate(reader).frame_ids['disliked']) == 1
