# caterpillar: Tools for searching an index and scoring the results
#
# Copyright (C) 2012-2013 Mammoth Labs
# Author: Kris Rogers <kris@mammothlabs.com.au>
from query import QueryEvaluator
from results import SearchHit, SearchResults
from scoring import TfidfScorer


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
        self.query_evaluator = QueryEvaluator(self.index)

    def count(self, query, text_field=None):
        """
        Return the number of frames matching the specified query.

        Required Arguments:
        query -- str value for query.

        Optional Arguments:
        text_field -- str name of text field to restrict search by.

        """
        return len(self._do_query(query, text_field).frame_ids)

    def filter(self, query, text_field=None):
        """
        Return a list of ids for frames that match the specified query.

        Required Arguments:
        query -- str value for query.

        Optional Arguments:
        text_field -- str name of text field to restrict search by.

        """
        return self._do_query(query, text_field).frame_ids

    def search(self, query, start=0, limit=25, text_field=None):
        """
        Return ranked frame data for frames that match the specified query.

        Required Arguments:
        query -- The query string

        Optional Arguments:
        start -- Start position for returned results.
        limit -- Number of results returned.
        text_field -- str name of text field to restrict search by.

        """
        query_result = self._do_query(query, text_field)
        hits = [SearchHit(fid, self.index.get_frame(fid)) for fid in query_result.frame_ids]
        num_matches = len(hits)
        if num_matches > 0:
            hits = self.scorer.score_and_rank(hits, query_result.matched_terms)[start:]

        if limit:
            hits = hits[:limit]

        return SearchResults(query, hits, num_matches, query_result.matched_terms)

    def _do_query(self, query, text_field=None):
        """
        Perform the query, optionally restricting to a specified text field.

        Returns an instance of ``QueryResult``.

        """
        return self.query_evaluator.evaluate(query, text_field)
