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
        self.num_frames = {}
        self.index = index
        self.idfs = {}

    def score_and_rank(self, hits, term_weights):
        """
        Score hits using a tf-idf weighting for terms present in a frame,
        multiplied by scaling co-efficients supplied in `term_weights`.

        """
        for hit in hits:
            score = 0
            try:
                num_frames = self.num_frames[hit.searched_field]
            except KeyError:
                num_frames = self.num_frames[hit.searched_field] = self.index.get_frame_count(hit.searched_field)
            for term in term_weights[hit.searched_field]:
                try:
                    tf = hit.tfs[term]
                except KeyError:
                    # term not present in frame
                    continue
                try:
                    idf = self.idfs[hit.searched_field][term]
                except KeyError:
                    # calculate & store term's idf
                    idf =  numpy.log(1 + num_frames / (self.index.get_term_frequency(term, hit.searched_field) + 1))
                    try: 
                        self.idfs[hit.searched_field][term] = idf
                    except KeyError: 
                        self.idfs[hit.searched_field] = {term: idf}
                score += term_weights[hit.searched_field][term] * tf * idf
            hit.score = score

        return sorted(hits, key=lambda h: (h.score, len(h.tfs), h.frame_id), reverse=True)
