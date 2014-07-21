# Copyright (C) Kapiche
# Author: Kris Rogers <kris@kapiche.com>
"""
This module exposes the ``IndexSearcher``, which allows searching an index for text frames.

"""
from caterpillar.searching.results import SearchHit, SearchResults
from caterpillar.searching.scoring import TfidfScorer
from caterpillar.searching.query import BaseQuery


class IndexSearcher(object):
    """
    Allows searching for text frames within the specified ``index``. Accepts a custom ``scorer_cls`` for use in ranking
    ``search`` results (defaults to tf-idf). Scorer must be of type `Scorer <caterpillar.searching.scoring.Scorer>`_.

    All searching operations expect an object of type `BaseQuery <caterpillar.searching.query.BaseQuery>`_.

    The ``count`` and ``filter`` methods expose the most efficient search operations. The ``search`` method
    must score and rank all of its results, so should only be used when interested in the ranking of results.

    """
    def __init__(self, index, scorer_cls=TfidfScorer):
        self.index = index
        self.scorer = scorer_cls(index)

    def count(self, query):
        """
        Return the number of frames matching the specified ``query`` (must be of type
        `BaseQuery <caterpillar.searching.query.BaseQuery>`_).

        """
        return len(self._do_query(query).frame_ids)

    def filter(self, query):
        """
        Return a list of ids for frames that match the specified ``query`` (must be of type
        `BaseQuery <caterpillar.searching.query.BaseQuery>`_).

        """
        return self._do_query(query).frame_ids

    def search(self, query, start=0, limit=25):
        """
        Return ranked frame data for frames that match the specified (must be of type
        `BaseQuery <caterpillar.searching.query.BaseQuery>`_).

        Note that the ranking of results is performed by a `Scorer <caterpillar.searching.Scorer>`_ that is initialised
        when the ``IndexSearcher`` is created.

        ``start`` and ``limit`` define pagination of results, which defaults to the first 25 frames.

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
            raise TypeError("`query` parameter must match type `caterpillar.searching.query.BaseQuery`")

        return query.evaluate(self.index)
