# Copyright (c) 2012-2014 Kapiche Limited
# Author: Kris Rogers <kris@kapiche.com>
"""
This module implements the query framework for caterpillar.

The design intends that the ``BaseQuery`` class is extended to provide specific query functionality that can be used by
`IndexSearcher <caterpillar.searching.IndexSearcher>`_.

"""
import abc


class QueryResult(object):
    """
    Encapsulates results for a query, including combined queries.

    The result of a query is a dict of text_field: set(frame_ids) and text_field: term_weights.

    All term weightings default to 1 unless they are modified explicitly by the query. Their purpose is to facilitate
    scoring a query result, based on the query that returned it.

    """

    def __init__(self, frame_ids, term_weights, text_field):
        self.frame_ids = {text_field: set(frame_ids)}
        self.term_weights = {text_field: term_weights}

    def __ior__(self, other_query):
        """ Union of this query and other_query. """
        for text_field, frame_ids in other_query.frame_ids.iteritems():
            try:
                self.frame_ids[text_field] |= frame_ids
            except KeyError:
                self.frame_ids[text_field] = frame_ids

        for text_field, term_weights in other_query.term_weights.iteritems():
            for term, weight in term_weights.iteritems():
                # Carry through the maximum term weight for all of the queries.
                try:
                    self.term_weights[text_field][term] = max(self.term_weights[text_field][term], weight)
                except KeyError:
                    try:
                        self.term_weights[text_field][term] = weight
                    except KeyError:
                        self.term_weights[text_field] = {term: weight}
        return self

    def __iand__(self, other_query):
        """ Intersection of this query and other_query. """

        # Need to combine in both directions, otherwise fields in self but not other_query will be missed.
        for text_field, frame_ids in self.frame_ids.iteritems():
            try:
                other_query.frame_ids[text_field] &= frame_ids
            except KeyError:
                other_query.frame_ids[text_field] = set()

        for text_field, frame_ids in other_query.frame_ids.iteritems():
            try:
                self.frame_ids[text_field] &= frame_ids
            except KeyError:
                self.frame_ids[text_field] = set()

        for text_field, term_weights in other_query.term_weights.iteritems():
            for term, weight in term_weights.iteritems():
                # Carry through the maximum term weight for all of the queries.
                try:
                    self.term_weights[text_field][term] = max(self.term_weights[text_field][term], weight)
                except KeyError:
                    try:
                        self.term_weights[text_field][term] = weight
                    except KeyError:
                        self.term_weights[text_field] = {term: weight}

        return self

    def __isub__(self, other_query):
        """Remove other_query results from this query"""
        for text_field, frame_ids in other_query.frame_ids.iteritems():
            try:
                self.frame_ids[text_field] -= frame_ids
            except KeyError:
                pass
        # Exclusions are treated as boolean only, and do not affect term weighting for scoring.
        return self


class QueryError(Exception):
    """Invalid query"""


class BaseQuery(object):
    """
    Each ``BaseQuery`` concrete class should represent an individual facet of the query API, providing query
    functionality in the ``evaluate`` method.

    """
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def evaluate(self, index):
        """
        Evaluate this query against the specified ``index``.

        Returns ``QueryResult``.

        Raises ``QueryError`` on exception.

        """
        raise NotImplementedError
