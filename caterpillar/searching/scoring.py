# caterpillar: Tools for scoring search results.
#
# Copyright (C) 2012-2013 Mammoth Labs
# Author: Kris Rogers <kris@mammothlabs.com.au>
import abc
import math


class Scorer(object):
    """
    Scorers calculate a numerical score for query hits to rank them by.

    """
    __metaclass__ = abc.ABCMeta

    def __init__(self, index):
        pass

    @abc.abstractmethod
    def score_and_rank(self, hits, term_weights):
        """
        Scorer each of the specified hits and return them in ranked order.

        Required Arguments:
        hits -- A list of ``SearchrHit`` objects.
        term_weights -- A list of term weights to use in scoring.

        """
        pass


class SimpleScorer(Scorer):
    """
    Simple scorer implementation to be used by ``IndexSearcher``.

    """
    def score_and_rank(self, hits, term_weights):
        """
        Simply score hits by the presence of query terms and their weighting.

        """
        for hit in hits:
            score = 0
            for term in hit.frame_terms.intersection(term_weights.keys()):
                score += term_weights[term]
            hit.score = score

        return sorted(hits, key=lambda h: h.score, reverse=True)


class TfidfScorer(Scorer):
    """
    A scorer that uses TF-IDF.

    """
    def __init__(self, index):
        self.num_frames = index.get_frame_count()
        self.term_positions = index.get_positions_index()
        super(TfidfScorer, self).__init__(index)

    def score_and_rank(self, hits, term_weights):
        """
        Score hits and return in ranked order according to TF-IDF.

        """
        idfs = self._compute_idfs(term_weights)
        for hit in hits:
            score = 0
            for term in hit.frame_terms.intersection(term_weights.keys()):
                score += len(self.term_positions[term][hit.frame_id]) * idfs[term] * term_weights[term]
            hit.score = score

        return sorted(hits, key=lambda h: h.score, reverse=True)

    def _compute_idfs(self, terms):
        """
        Compute inverse document frequencies for the specified terms.

        """
        return {term: math.log(self.num_frames / len(self.term_positions[term])) for term in terms}
