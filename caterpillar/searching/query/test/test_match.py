# Copyright (C) 2012-2014 Kapiche
# Author: Kris Rogers <kris@kapiche.com>
"""Tests for the match module."""
from caterpillar.searching.query import BaseQuery, QueryResult
from caterpillar.searching.query.match import MatchAllQuery, MatchSomeQuery


class MockTestQuery(BaseQuery):
    """
    A contrived query that simply returns the ``frames`` and ``term_weights`` passed into the ``__init__`` method.

    """
    def __init__(self, frames, text_field='text', term_weights=None):
        self.frames = set(frames)
        self.term_weights = term_weights or {}
        self.text_field = text_field

    def evaluate(self, index):
        return QueryResult(self.frames, self.term_weights, self.text_field)


def test_match_all():
    """Test match all query."""
    # Simple match all

    r = MatchAllQuery([MockTestQuery([1, 2, 3], term_weights={'a': 1}),
                       MockTestQuery([1, 3], term_weights={'b': 99})]).evaluate(None)
    assert r.frame_ids['text'] == set([1, 3])
    assert r.term_weights['text'] == {'a': 1, 'b': 99}
    # Match all with exclude queries
    r = MatchAllQuery([MockTestQuery([1, 2, 3]), MockTestQuery([1, 2, 3])],
                      [MockTestQuery([1]), MockTestQuery([2])]).evaluate(None)
    assert r.frame_ids['text'] == set([3])

    # Match all with exclude queries from different fields.
    r = MatchAllQuery([MockTestQuery([1, 2, 3]), MockTestQuery([1, 2, 3])],
                      [MockTestQuery([1], text_field='text2'), MockTestQuery([2])]).evaluate(None)
    assert r.frame_ids['text'] == set([1, 3])

    # Combining queries on different fields should result in empty sets.
    r = MatchAllQuery([MockTestQuery([1, 2, 3], term_weights={'a': 1}, text_field='text1'),
                       MockTestQuery([1, 3], term_weights={'b': 99}, text_field='text2')]).evaluate(None)
    assert r.frame_ids['text1'] == set()
    assert r.frame_ids['text2'] == set()


def test_match_some():
    """Test match some query."""
    # Simple match some
    r = MatchSomeQuery([MockTestQuery([1, 3, 5], term_weights={'a': 1}),
                        MockTestQuery([2, 4, 6], term_weights={'a': 1.5})]).evaluate(None)
    assert r.frame_ids['text'] == set([1, 2, 3, 4, 5, 6])
    assert r.term_weights['text'] == {'a': 1.5}
    # Match some with exclude queries
    r = MatchSomeQuery([MockTestQuery([1]), MockTestQuery([2]), MockTestQuery([3])],
                       [MockTestQuery([1]), MockTestQuery([2])]).evaluate(None)
    assert r.frame_ids['text'] == set([3])

    # Test with different text_fields in each query.
    r = MatchSomeQuery([MockTestQuery([1, 2, 3], term_weights={'a': 1}, text_field='text1'),
                       MockTestQuery([1, 3], term_weights={'b': 99}, text_field='text2')]).evaluate(None)
    assert r.frame_ids['text1'] == set([1, 2, 3])
    assert r.frame_ids['text2'] == set([1, 3])
