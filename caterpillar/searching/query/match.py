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
    def __init__(self, queries, intersection, exclude_queries=None):
        self.queries = queries
        self.intersection = intersection
        self.exclude_queries = exclude_queries

    def evaluate(self, index):
        results = [q.evaluate(index) for q in self.queries]
        query_result = self._merge_query_results(results)
        if self.exclude_queries:
            self._exclude_query_results(query_result, [q.evaluate(index) for q in self.exclude_queries])
        return query_result

    @classmethod
    def _exclude_query_results(cls, result, exclude_results):
        """
        Take the difference between an existing query result and a list of results to exclude.

        """
        for other_result in exclude_results:
            result.frame_ids = result.frame_ids.difference(other_result.frame_ids)

    def _merge_query_results(self, results):
        """
        Merge the specified query results together, optionally by their intersection.

        """
        result = results[0]
        for other_result in results[1:]:
            # Merge frame ids
            if self.intersection:
                result.frame_ids.intersection_update(other_result.frame_ids)
            else:
                result.frame_ids.update(other_result.frame_ids)
            # Merge term weights
            for term, weight in other_result.term_weights.iteritems():
                if term in result.term_weights:
                    result.term_weights[term] = max(result.term_weights[term], weight)
                else:
                    result.term_weights[term] = weight

        return result


class MatchAllQuery(_MatchQuery):
    """
    The match all query performs an intersection across a list of ``queries`` (of type ``BaseQuery``), optionally
    subtracting the results of ``exclude_queries``.

    """
    def __init__(self, queries, exclude_queries=None):
        super(MatchAllQuery, self).__init__(queries, True, exclude_queries)


class MatchSomeQuery(_MatchQuery):
    """
    The match some query performs a union across a list of ``queries`` (of type ``BaseQuery``), optionally excluding the
    results of ``exclude_queries``.

    """
    def __init__(self, queries, exclude_queries=None):
        super(MatchSomeQuery, self).__init__(queries, False, exclude_queries)
