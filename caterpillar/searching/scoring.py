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

        return sorted(hits, key=lambda h: (h.score, len(h.frame_terms), h.frame_id), reverse=True)


class TfidfScorer(Scorer):
    """
    A scorer that uses TF-IDF in a Vector Space Model.

    """
    def __init__(self, index):
        self.frame_indexes = {frame_id: i for i, frame_id in enumerate(list(index.get_frame_ids()))}
        self.term_positions = {k: v for k, v in index.get_positions_index()}
        self.tfidf_index, self.tfidf_index_norm =TfidfScorer.build_tfidf_index(self.term_positions,
            self.frame_indexes)
        super(TfidfScorer, self).__init__(index)

    @staticmethod
    def build_tfidf_index(term_positions, frame_indexes):
        """
        Given `term_positions` and `frame_indexes`, which maps each frame id to a numerical index,
        generate a full tf-idf index for use in searching.

        Returns a 2-tuple containing the generated tf-idf index, and,
        a vector containing the norms of all frame vectors in the tf-idf index.

        """
        num_frames = len(frame_indexes)
        idfs = numpy.array([
            numpy.log((num_frames / len(positions) if positions else 0) + 1)
            for positions in term_positions.itervalues()
        ])
        tfs = numpy.zeros((len(term_positions), num_frames))
        term_index = 0
        for term, frame_positions in term_positions.iteritems():
            for frame_id, positions in frame_positions.iteritems():
                tfs[term_index][frame_indexes[frame_id]] = len(positions)
            term_index = term_index + 1

        tfidf_index = tfs.T * idfs

        return (tfidf_index, numpy.linalg.norm(tfidf_index, axis=1))

    def score_and_rank(self, hits, term_weights):
        """
        Score hits and return in ranked order according to TF-IDF in the VSM.

        """
        if len(term_weights) == 0:
            # Handle metadata-only search where no scoring is necessary
            return hits

        # Calculate cosine similarity
        query_weights = []
        for term in self.term_positions.iterkeys():
            query_weights.append(term_weights.get(term, 0))
        scored_hits = numpy.dot(self.tfidf_index, query_weights) / (self.tfidf_index_norm *
                                                                   numpy.linalg.norm(query_weights))

        # Update scores and return ranked hits
        for hit in hits:
            hit.score = scored_hits[self.frame_indexes[hit.frame_id]]
        return sorted(hits, key=lambda h: (h.score, len(h.frame_terms), h.frame_id), reverse=True)
