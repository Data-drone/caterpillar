# caterpillar: Tests for the caterpillar.processing.index module
#
# Copyright (C) 2012-2013 Mammoth Labs
# Author: Kris Rogers <kris@mammothlabs.com.au>
from __future__ import division
import os

from caterpillar.processing.analysis.analyse import DefaultTestAnalyser
from caterpillar.processing.frames import frame_stream
from caterpillar.processing.index import *


#### Functional tests ####
def test_index_alice():
    with open(os.path.abspath('caterpillar/resources/alice_test_data.txt'), 'r') as f:
        index = build_text_index(frame_stream(f, meta_data={'document': 'alice_test_data.txt'}),
                                 analyser=DefaultTestAnalyser(), fold_case=False)

        assert len(index.term_positions['nice']) == 3
        assert len(index.term_positions['key']) == 5

        assert index.term_associations['Alice']['poor'] == index.term_associations['poor']['Alice'] == 3
        assert index.term_associations['key']['golden'] == index.term_associations['golden']['key'] == 3

        assert len(index.term_frequencies_by_frame) == 504
        assert index.term_frequencies_by_frame.N() == 871
        assert index.term_frequencies_by_frame['Alice'] == 23


def test_index_alice_with_bigram_words():
    with open(os.path.abspath('caterpillar/resources/alice.txt'), 'r') as f:
        bi_grams = find_bi_gram_words(frame_stream(f))
        f.seek(0)
        index = build_text_index_with_bi_grams(frame_stream(f), bi_grams, case_folding=True)

        assert len(bi_grams) == 5
        assert 'golden key' in bi_grams
        assert index.term_frequencies_by_frame['golden key'] == 6
        assert index.term_frequencies_by_frame['golden'] == 1
        assert index.term_frequencies_by_frame['key'] == 3


def test_index_moby_with_case_folding():
    with open(os.path.abspath('caterpillar/resources/moby.txt'), 'r') as f:
        index = build_text_index(frame_stream(f), analyser=DefaultTestAnalyser(), fold_case=True)

        assert 'flask' not in index.term_frequencies_by_frame
        assert index.term_frequencies_by_frame['Flask'] == 92
        assert index.term_associations['Flask']['person'] == index.term_associations['person']['Flask'] == 3

        assert 'Well' not in index.term_frequencies_by_frame
        assert index.term_frequencies_by_frame['well'] == 208
        assert index.term_associations['well']['whale'] == index.term_associations['whale']['well'] == 26

        assert len(index.term_frequencies_by_frame) == 17912
        assert index.term_frequencies_by_frame.N() == 104526


def test_find_bigram_words():
    with open(os.path.abspath('caterpillar/resources/moby.txt'), 'r') as f:
        bi_grams = find_bi_gram_words(frame_stream(f))
        f.seek(0)
        index = build_text_index_with_bi_grams(frame_stream(f), bi_grams, case_folding=True)

        assert len(bi_grams) == 3
        assert 'vinegar cruet' in bi_grams
        assert index.term_frequencies_by_frame['vinegar cruet'] == 4
        assert 'vinegar' not in index.term_frequencies_by_frame
        assert 'cruet' not in index.term_frequencies_by_frame