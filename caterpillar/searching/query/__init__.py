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
    Encapsulates result data for a single query, which is comprised of a list of ``frame_ids`` that match the query, and
    ``term_weights``, which is a dit of *matched* query terms to their float weightings. All weightings default to 1
    unless they are modified explicitly by the query. Their purpose is to facilitate scoring a query result, based
    on the query that returned it.

    """
    def __init__(self, frame_ids, term_weights):
        self.frame_ids = frame_ids
        self.term_weights = term_weights


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
