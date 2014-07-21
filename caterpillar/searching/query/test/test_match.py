# Copyright (C) Kapiche
# Author: Kris Rogers <kris@kapiche.com>
"""Tests for the match module."""
from caterpillar.searching.query import BaseQuery, QueryResult
from caterpillar.searching.query.match import MatchAllQuery, MatchSomeQuery


class MockTestQuery(BaseQuery):
    """
    A contrived query that simply returns the ``frames`` and ``term_weights`` passed into the ``__init__`` method.

    """
    def __init__(self, frames, term_weights=None):
        self.frames = set(frames)
        self.term_weights = term_weights or {}

    def evaluate(self, index):
        return QueryResult(self.frames, self.term_weights)


def test_match_all():
    """Test match all query."""
    # Simple match all
    r = MatchAllQuery([MockTestQuery([1, 2, 3], {'a': 1}), MockTestQuery([1, 3], {'b': 99})]).evaluate(None)
    assert r.frame_ids == set([1, 3])
    assert r.term_weights == {'a': 1, 'b': 99}
    # Match all with exclude queries
    r = MatchAllQuery([MockTestQuery([1, 2, 3]), MockTestQuery([1, 2, 3])],
                      [MockTestQuery([1]), MockTestQuery([2])]).evaluate(None)
    assert r.frame_ids == set([3])


def test_match_some():
    """Test match some query."""
    # Simple match some
    r = MatchSomeQuery([MockTestQuery([1, 3, 5], {'a': 1}), MockTestQuery([2, 4, 6], {'a': 1.5})]).evaluate(None)
    assert r.frame_ids == set([1, 2, 3, 4, 5, 6])
    assert r.term_weights == {'a': 1.5}
    # Match some with exclude queries
    r = MatchSomeQuery([MockTestQuery([1]), MockTestQuery([2]), MockTestQuery([3])],
                       [MockTestQuery([1]), MockTestQuery([2])]).evaluate(None)
    assert r.frame_ids == set([3])
