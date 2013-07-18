# caterpillar: Tests for the caterpillar.processing.index module
#
# Copyright (C) 2012-2013 Mammoth Labs
# Author: Kris Rogers <kris@mammothlabs.com.au>
import os

from caterpillar.processing.frames import frame_stream
from caterpillar.processing.index import *
from caterpillar.processing.tokenize import StopwordTokenFilter
from caterpillar.processing import stopwords


def test_index_alice():
    frames = list(frame_stream(open(os.path.abspath('caterpillar/processing/test/alice_test_data.txt'), 'r'),
                               meta_data={'document': 'alice_test_data.txt'}))
    index = build_text_index(frames, filters=[StopwordTokenFilter(stopwords.ENGLISH_TEST, stopwords.MIN_WORD_SIZE)])

    assert len(index.term_positions['nice']) == 3
    assert len(index.term_positions['key']) == 5

    assert index.term_associations['Alice']['poor'] == index.term_associations['poor']['Alice'] == 3
    assert index.term_associations['key']['golden'] == index.term_associations['golden']['key'] == 3

    assert len(index.term_frequencies_by_frame) == 510
    assert index.term_frequencies_by_frame.N() == 869
    assert index.term_frequencies_by_frame['Alice'] == 24


def test_index_alice_with_bigram_words():
    frames = list(frame_stream(open(os.path.abspath('examples/alice.txt'), 'r')))
    bigrams, frame_bigrams = find_bigram_words(frames)

    frames = frame_stream(open(os.path.abspath('examples/alice.txt'), 'r'))
    index = build_text_index(frames, frame_ngrams=frame_bigrams)
    assert index.term_frequencies_by_frame['golden key'] == 6


def test_index_moby_with_case_folding():
    frames = list(frame_stream(open(os.path.abspath('examples/moby.txt'), 'r')))
    index = build_text_index(frames, filters=[StopwordTokenFilter(stopwords.ENGLISH_TEST, stopwords.MIN_WORD_SIZE)], case_folding=True)
    assert 'flask' not in index.term_frequencies_by_frame
    assert index.term_frequencies_by_frame['Flask'] == 87
    assert 'Well' not in index.term_frequencies_by_frame
    assert index.term_frequencies_by_frame['well'] == 211


def test_find_bigram_words():
    frames = frame_stream(open(os.path.abspath('examples/moby.txt'), 'r'))

    bigrams, frame_bigrams = find_bigram_words(frames)
    assert ('vinegar', 'cruet') in bigrams
    assert len(bigrams) == 1