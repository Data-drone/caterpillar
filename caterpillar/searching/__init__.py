# Copyright (C) Kapiche
# Author: Kris Rogers <kris@kapiche.com>
"""
Tools for searching an index and scoring the results.

"""
from caterpillar.searching.results import SearchHit, SearchResults
from caterpillar.searching.scoring import TfidfScorer
from caterpillar.searching.query import BaseQuery


class IndexSearcher(object):
    """
    Allows searching for text frames for a specific index.

    Required Arguments:
    index -- The index to intialise this searcher for.
    scorer_cls -- The type of scorer to use in ranking text frames.

    """
    def __init__(self, index, scorer_cls=TfidfScorer):
        self.index = index
        self.scorer = scorer_cls(index)

    def count(self, query):
        """
        Return the number of frames matching the specified query.

        Required Arguments:
        query -- A query object based on ``caterpillar.searching.query.BaseQuery``.

        """
        return len(self._do_query(query).frame_ids)

    def filter(self, query):
        """
        Return a list of ids for frames that match the specified query.

        Required Arguments:
        query -- A query object based on ``caterpillar.searching.query.BaseQuery``.

        """
        return self._do_query(query).frame_ids

    def search(self, query, start=0, limit=25):
        """
        Return ranked frame data for frames that match the specified query.

        Required Arguments:
        query -- A query object based on ``caterpillar.searching.query.BaseQuery``.

        Optional Arguments:
        start -- Start position for returned results.
        limit -- Number of results returned.

        """
        query_result = self._do_query(query)
        hits = [SearchHit(fid, self.index.get_frame(fid)) for fid in query_result.frame_ids]
        num_matches = len(hits)
        if num_matches > 0:
            hits = self.scorer.score_and_rank(hits, query_result.term_weights)[start:]

        if limit:
            hits = hits[:limit]

        return SearchResults(query, hits, num_matches, query_result.term_weights)

    def _do_query(self, query):
        """Perform the actual query."""
        if not isinstance(query, BaseQuery):
            raise ValueError("`query` parameter must match type `caterpillar.searching.query.BaseQuery`")

        return query.evaluate(self.index)
