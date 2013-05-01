# caterpillar: Tests for the caterpillar.analysis.frame module
#
# Copyright (C) 2012-2013 Mammoth Labs
# Author: Ryan Stuart <ryan@mammothlabs.com.au>


import os
from caterpillar.analysis.frames import frame_stream


def test_frame_stream_alice():
    frames = list(frame_stream(open(os.path.abspath('caterpillar/analysis/test/alice_test_data.txt'), 'r'),
                               meta_data={'document': 'alice_test_data.txt'}))
    assert len(frames) == 49
    assert frames[0].metadata['document'] == 'alice_test_data.txt'


def test_frame_stream_economics():
    frames = list(frame_stream(open(os.path.abspath('caterpillar/analysis/test/economics_test_data.txt'), 'r'),
                               meta_data={'document': 'economics_test_data.txt'}))
    assert len(frames) == 7
    assert frames[0].metadata['document'] == 'economics_test_data.txt'