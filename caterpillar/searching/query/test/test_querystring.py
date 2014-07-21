# Copyright (C) Kapiche
# Author: Kris Rogers <kris@kapiche.com>
"""Tests for the querystring module."""
import os

from caterpillar.processing.index import Index
from caterpillar.processing import schema
from caterpillar.searching.query.querystring import QueryStringQuery


def test_querystring_query_basic():
    """Test querystring query basic functionality."""
    with open(os.path.abspath('caterpillar/test_resources/alice.txt'), 'rbU') as f:
        data = f.read()
    index = Index.create(schema.Schema(text=schema.TEXT))
    index.add_document(text=data)
    # Simple terms
    alice_count = len(QueryStringQuery('Alice').evaluate(index).frame_ids)
    king_count = len(QueryStringQuery('King').evaluate(index).frame_ids)
    assert alice_count > 0
    assert king_count > 0
    # Boolean operators
    alice_and_king_count = len(QueryStringQuery('Alice AND King').evaluate(index).frame_ids)
    alice_not_king_count = len(QueryStringQuery('Alice NOT King').evaluate(index).frame_ids)
    alice_or_king_count = len(QueryStringQuery('Alice OR King').evaluate(index).frame_ids)
    king_not_alice_count = len(QueryStringQuery('King NOT Alice').evaluate(index).frame_ids)
    assert alice_not_king_count == alice_count - alice_and_king_count
    assert king_not_alice_count == king_count - alice_and_king_count
    assert alice_or_king_count == alice_not_king_count + king_not_alice_count + alice_and_king_count
    # Wildcards
    assert len(QueryStringQuery('*ice').evaluate(index).frame_ids) > alice_count
    assert len(QueryStringQuery('K??g').evaluate(index).frame_ids) == king_count

def test_querystring_query_advanced():
    """Test querysting query advanced searching."""
    index = Index.create(schema.Schema(liked=schema.TEXT, disliked=schema.TEXT, age=schema.NUMERIC(indexed=True),
                                       gender=schema.CATEGORICAL_TEXT(indexed=True)))
    index.add_document(liked='product', disliked='service', age=20, gender='male')
    index.add_document(liked='service', disliked='product', age=30, gender='male')
    index.add_document(liked='service', disliked='price', age=40, gender='female')
    index.add_document(liked='product', disliked='product', age=80, gender='female')
    # Metadata
    assert len(QueryStringQuery('age=80').evaluate(index).frame_ids) == 2
    assert len(QueryStringQuery('age<80').evaluate(index).frame_ids) == 6
    assert len(QueryStringQuery('age>=20').evaluate(index).frame_ids) == 8
    assert len(QueryStringQuery('product not gender=male').evaluate(index).frame_ids) == 2
    assert len(QueryStringQuery('product not gender=*male').evaluate(index).frame_ids) == 0
    # Text field
    assert len(QueryStringQuery('product', 'liked').evaluate(index).frame_ids) == 2
    assert len(QueryStringQuery('gender=female not product', 'disliked').evaluate(index).frame_ids) == 1
    
