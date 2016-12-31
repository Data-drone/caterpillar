# Copyright (c) 2012-2014 Kapiche Limited
# Author: Kris Rogers <kris@kapiche.com>, Ryan Stuart <ryan@kapiche.com>
"""This module exposes the :class:`.IndexSearcher`, which allows searching an index for text frames."""
from caterpillar.searching.results import SearchHit, SearchResults
from caterpillar.searching.scoring import TfidfScorer
from caterpillar.searching.query import BaseQuery


class IndexSearcher(object):
    """
    Allows searching for text frames within the specified ``index_reader``. Accepts a custom ``scorer_cls`` for use in
    ranking ``search`` results (defaults to tf-idf). Scorer must be of type
    `Scorer <caterpillar.searching.scoring.Scorer>`_.

    All searching operations expect an object of type `BaseQuery <caterpillar.searching.query.BaseQuery>`_.

    The ``count`` and ``filter`` methods expose the most efficient search operations. The ``search`` method
    must score and rank all of its results, so should only be used when interested in the ranking of results.

    """
    def __init__(self, index_reader, scorer_cls=TfidfScorer):
        self.index_reader = index_reader
        self.scorer = scorer_cls

    def count(self, query):
        """
        Return the number of frames matching the specified ``query`` (must be of type
        `BaseQuery <caterpillar.searching.query.BaseQuery>`_).

        """
        return sum(len(frame_ids) for field, frame_ids in self._do_query(query).frame_ids.iteritems())

    def filter(self, query):
        """
        Return a list of (field, frame_id) pairs for frames that match the specified ``query`` (must be of type
        `BaseQuery <caterpillar.searching.query.BaseQuery>`_).

        """
        results = []
        for field, frame_ids in self._do_query(query).frame_ids.iteritems():
            results.extend([(field, frame_id) for frame_id in frame_ids])
        return results

    def search(self, query, start=0, limit=25):
        """
        Return ranked frame data for frames that match the specified (must be of type
        `BaseQuery <caterpillar.searching.query.BaseQuery>`_).

        Note that the ranking of results is performed by a `Scorer <caterpillar.searching.Scorer>`_ that is initialised
        when the ``IndexSearcher`` is created.

        ``start`` and ``limit`` define pagination of results, which defaults to the first 25 frames.

        """
        query_scorer = self.scorer(self.index_reader)
        query_result = self._do_query(query)

        hits = []
        for text_field, frame_ids in query_result.frame_ids.iteritems():
            for fid in frame_ids:
                hits.append(SearchHit(
                    fid, text_field, self.index_reader.get_frame(fid, text_field),
                    query_result.term_frequencies[text_field][fid]
                ))

        num_matches = len(hits)
        if num_matches > 0:
            hits = query_scorer.score_and_rank(hits, query_result.term_weights)[start:]

        if limit:
            hits = hits[:limit]

        return SearchResults(query, hits, num_matches, query_result.term_weights)

    def _do_query(self, query):
        """Perform the actual query."""
        if not isinstance(query, BaseQuery):
            raise TypeError("`query` parameter must match type `caterpillar.searching.query.BaseQuery`")

        return query.evaluate(self.index_reader)
