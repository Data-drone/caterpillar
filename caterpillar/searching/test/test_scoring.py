# Copyright (c) 2012-2014 Kapiche Limited
# Author: Kris Rogers <kris@kapiche.com>
"""Tests for scoring module."""
from __future__ import division
import math

from caterpillar import abstract_method_tester
from caterpillar.searching.scoring import Scorer, TfidfScorer


def test_scoring_abc():
    """This is crap but necessary to get 100% coverage :("""
    abstract_method_tester(Scorer)


def test_tfidf_index():
    """Test generation of tf-idf index with simple, contrived example."""
    term_positions = {
        "bicycle": {
            "frame1": [0, 2, 6],
            "frame3": [0]
        },
        "bus": {
            "frame3": [3]
        },
        "car": {
            "frame2": [1]
        }
    }
    frame_indexes = {
        "frame1": 0,
        "frame2": 1,
        "frame3": 2
    }
    tfidf_index, tfidf_frame_norm = TfidfScorer.build_tfidf_index(term_positions, frame_indexes)
    reverese_frame_indexes = {index: frame_id for frame_id, index in frame_indexes.iteritems()}
    terms = term_positions.keys()
    num_frames = len(frame_indexes)
    for i, frame_vector in enumerate(tfidf_index):
        frame_id = reverese_frame_indexes[i]
        for j, value in enumerate(frame_vector):
            term = terms[j]
            if frame_id == "frame1":
                if term == "bicycle":
                    assert value == 3 * math.log(1 + num_frames / (2 + 1))
                else:
                    assert value == 0
            elif frame_id == "frame2":
                if term == "car":
                    assert value == 1 * math.log(1 + num_frames / (1 + 1))
                else:
                    assert value == 0
            elif frame_id == "frame3":
                if term == "bicycle":
                    assert value == 1 * math.log(1 + num_frames / (2 + 1))
                elif term == "bus":
                    assert value == 1 * math.log(1 + num_frames / (1 + 1))
                else:
                    assert value == 0
