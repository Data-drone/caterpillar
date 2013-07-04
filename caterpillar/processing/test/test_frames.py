# caterpillar: Tests for the caterpillar.processing.frames module
#
# Copyright (C) 2012-2013 Mammoth Labs
# Author: Ryan Stuart <ryan@mammothlabs.com.au>


import os
from caterpillar.processing.frames import *


def test_frame_stream_alice():
    """Test frame extraction on Chapter 1 of Alice in Wonderland."""
    frames = list(frame_stream(open(os.path.abspath('caterpillar/processing/test/alice_test_data.txt'), 'r'),
                               meta_data={'document': 'alice_test_data.txt'}))
    assert len(frames) == 49
    assert frames[0].metadata['document'] == 'alice_test_data.txt'


def test_frame_stream_economics():
    """Test frame extraction on a Wikipedia page about Economics."""
    frames = list(frame_stream(open(os.path.abspath('caterpillar/processing/test/economics_test_data.txt'), 'r'),
                               meta_data={'document': 'economics_test_data.txt'}))
    assert len(frames) == 7
    assert frames[0].metadata['document'] == 'economics_test_data.txt'


def test_frame_stream_csv_regular():
    """Test CSV frame extraction using some customer feedback data."""
    columns = [ColumnSpec('respondant', ColumnDataType.INTEGER),
               ColumnSpec('region', ColumnDataType.STRING),
               ColumnSpec('store', ColumnDataType.STRING),
               ColumnSpec('liked', ColumnDataType.TEXT),
               ColumnSpec('disliked', ColumnDataType.TEXT),
               ColumnSpec('would_like', ColumnDataType.TEXT),
               ColumnSpec('nps', ColumnDataType.INTEGER)]
    frames = list(frame_stream_csv(open(os.path.abspath('caterpillar/processing/test/test_small.csv'), 'rbU'), columns,
                  meta_data={'document': 'test_small.csv'}))
    assert len(frames) == 39
    assert frames[0].metadata['document'] == 'test_small.csv'
    assert frames[0].metadata['row_seq'] == '1'
    assert frames[14].metadata['nps'] == '1'


def test_frame_stream_csv_with_cr():
    """Test CSV frame extraction using customer feedback data with OSX style newline characters."""
    columns = [ColumnSpec('respondant', ColumnDataType.INTEGER),
               ColumnSpec('region', ColumnDataType.STRING),
               ColumnSpec('store', ColumnDataType.STRING),
               ColumnSpec('liked', ColumnDataType.TEXT),
               ColumnSpec('disliked', ColumnDataType.TEXT),
               ColumnSpec('would_like', ColumnDataType.TEXT),
               ColumnSpec('nps', ColumnDataType.INTEGER)]
    frames = list(frame_stream_csv(open(os.path.abspath('caterpillar/processing/test/test_with_CR.csv'), 'rbU'), columns,
                  meta_data={'document': 'test_small.csv'}))
    assert len(frames) == 39
    assert frames[0].metadata['document'] == 'test_small.csv'
    assert frames[0].metadata['row_seq'] == '1'
    assert frames[14].metadata['nps'] == '1'


def test_frame_stream_csv_cell_as_frame():
    """Text CSV frame extraction with frame size set to 0 (treat each cell as a frame no matter the length)."""
    columns = [ColumnSpec('respondant', ColumnDataType.INTEGER),
               ColumnSpec('region', ColumnDataType.STRING),
               ColumnSpec('store', ColumnDataType.STRING),
               ColumnSpec('liked', ColumnDataType.TEXT),
               ColumnSpec('disliked', ColumnDataType.TEXT),
               ColumnSpec('would_like', ColumnDataType.TEXT),
               ColumnSpec('nps', ColumnDataType.INTEGER)]
    frames = list(frame_stream_csv(open(os.path.abspath('caterpillar/processing/test/test_small.csv'), 'rbU'), columns,
                  meta_data={'document': 'test_small.csv'}, frame_size=0))
    assert len(frames) == 35
    assert frames[0].metadata['document'] == 'test_small.csv'
    assert frames[0].metadata['row_seq'] == '1'
    assert frames[14].metadata['nps'] == '1'