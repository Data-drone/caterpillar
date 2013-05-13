# caterpillar: Tests for the caterpillar.analysis.index module
#
# Copyright (C) 2012-2013 Mammoth Labs
# Author: Kris Rogers <kris@mammothlabs.com.au>
import os

from caterpillar.analysis.frames import frame_stream
from caterpillar.analysis.index import build_text_index


def test_index_alice():
    frames = list(frame_stream(open(os.path.abspath('caterpillar/analysis/test/alice_test_data.txt'), 'r'),
                               meta_data={'document': 'alice_test_data.txt'}))
    index = build_text_index(frames)

    assert len(index.term_positions['nice']) == 3
    assert len(index.term_positions['key']) == 5

    assert index.term_associations['Alice']['poor'] == index.term_associations['poor']['Alice'] == 3
    assert index.term_associations['key']['golden'] == index.term_associations['golden']['key'] == 3