# Copyright (c) 2012-2014 Kapiche Limited
# Author: Kris Rogers <kris@kapiche.com>
"""
The purpose of this module is to allow the matching of arbitrary combinations of other queries when searching. This is
of particular use in combining the core query functionality of
`QueryStringQuery <caterpillar.searching.querystring.QueryStringQuery>`_ with various plugin-provided queries.

Callers should use either :class:`MatchAllQuery`` or :class:`MatchSomeQuery` to match the results of 1 or more
`BaseQuery <caterpillar.searching.query.BaseQuery>`_ objects.

Also note that it is possible to nest ``MatchAllQuery`` and ``MatchSomeQuery`` objects within themselves and each other.

"""
from caterpillar.searching.query import BaseQuery, QueryResult, QueryError


class _MatchQuery(BaseQuery):
    """
    Implement functionality for matching a list of queries via intersection (boolean and) or union (boolean or).

    For public usage, see :class:`MatchAllQuery` and :class:`MatchSomeQuery`.

    Expects a list of ``queries`` that must be of type ``BaseQuery``. ``intersection`` specifies to join queries using
    intersection, otherwise union.

    Optionally accepts a list of ``exclude_queries`` whose results are subtracted from the matched queries.

    """
    def __init__(self, queries, intersection, exclude_queries):
        self.queries = queries
        self.intersection = intersection
        self.exclude_queries = exclude_queries

    def evaluate(self, index):
        """Evaluate all queries and combine the results into a single query. 

        """
        results = (q.evaluate(index) for q in self.queries)
        query_result = next(results)

        for other_result in results:
            if self.intersection:
                query_result &= other_result
            else:
                query_result |= other_result

        for exclude_result in (q.evaluate(index) for q in self.exclude_queries):
            query_result -= exclude_result

        return query_result


class MatchAllQuery(_MatchQuery):
    """
    The match all query performs an intersection across a list of ``queries`` (of type ``BaseQuery``), optionally
    subtracting the results of ``exclude_queries``.

    """
    def __init__(self, queries, exclude_queries=[]):
        super(MatchAllQuery, self).__init__(queries, True, exclude_queries)


class MatchSomeQuery(_MatchQuery):
    """
    The match some query performs a union across a list of ``queries`` (of type ``BaseQuery``), optionally excluding the
    results of ``exclude_queries``.

    """
    def __init__(self, queries, exclude_queries=[]):
        super(MatchSomeQuery, self).__init__(queries, False, exclude_queries)
