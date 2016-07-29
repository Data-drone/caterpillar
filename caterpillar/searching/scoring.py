# Copyright (c) 2012-201 Kapiche Limited
# Author: Kris Rogers <kris@kapiche.com>, Ryan Stuart <ryan@kapiche.com>
from __future__ import division
import abc

import numpy


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


class TfidfScorer(Scorer):
    """
    Simple tf-idf scorer implementation to be used by ``IndexSearcher``.

    """
    def __init__(self, index):
        self.num_frames = index.get_frame_count()
        self.index = index
        self.idfs = {}

    def score_and_rank(self, hits, term_weights):
        """
        Score hits using a tf-idf weighting for terms present in a frame,
        multiplied by scaling co-efficients supplied in `term_weights`.

        """
        for hit in hits:
            score = 0
            for term in term_weights:
                try:
                    tf = hit.tfs[term]
                except KeyError:
                    # term not present in frame
                    continue
                try:
                    idf = self.idfs[term]
                except KeyError:
                    # calculate & store term's idf
                    idf = self.idfs[term] = numpy.log(1 + self.num_frames / (self.index.get_term_frequency(term) + 1))
                score += term_weights[term] * tf * idf
            hit.score = score

        return sorted(hits, key=lambda h: (h.score, len(h.tfs), h.frame_id), reverse=True)
