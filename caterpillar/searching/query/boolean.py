# Copyright (C) Kapiche
# Author: Kris Rogers <kris@kapiche.com>
"""
This module implements boolean queries that can be used to join together other queries.

"""
from caterpillar.searching.query import BaseQuery, QueryResult, QueryError


class _JoinQuery(BaseQuery):
    """
    Implement functionality for joining a list of queries via intersection (boolan and) or union (boolean or).

    For public usage, see ``MatchAllQuery`` and ``MatchSomeQuery``.

    Required Arguments:
    queries -- List of query objects to join. Must be of type ``BaseQuery``.
    intersection -- True-like to join using intersection, otherwise union.

    Optional Arguments:
    exclude_queries -- List of queries to exclude from result set.

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


class MatchAllQuery(_JoinQuery):
    """
    The match all query performs an intersection across a list of queries, optionally excluding the results of another
    list of queries.

    Required Arguments:
    queries -- List of queries to match. Must be of type ``BaseQuery``.

    Optional Arguments:
    exclude_queries -- List of queries to exclude. Must be of type ``BaseQuery``.

    """
    def __init__(self, queries, exclude_queries=None):
        super(MatchAllQuery, self).__init__(queries, True, exclude_queries)


class MatchSomeQuery(_JoinQuery):
    """
    The match some query performs a union across a list of queries, optionally excluding the results of another list of
    queries.

    Required Arguments:
    queries -- List of queries to match. Must be of type ``BaseQuery``.

    Optional Arguments:
    exclude_queries -- List of queries to exclude. Must be of type ``BaseQuery``.

    """
    def __init__(self, queries, exclude_queries=None):
        super(MatchSomeQuery, self).__init__(queries, False, exclude_queries)
