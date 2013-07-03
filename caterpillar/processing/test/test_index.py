# caterpillar: Tests for the caterpillar.processing.index module
#
# Copyright (C) 2012-2013 Mammoth Labs
# Author: Kris Rogers <kris@mammothlabs.com.au>
import os

from caterpillar.processing.frames import frame_stream
from caterpillar.processing.index import build_text_index
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