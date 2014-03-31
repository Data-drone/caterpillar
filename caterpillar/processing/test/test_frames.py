# caterpillar: Tests for the caterpillar.processing.frames module
#
# Copyright (C) 2012-2013 Mammoth Labs
# Author: Ryan Stuart <ryan@mammothlabs.com.au>
import os
from caterpillar.processing.frames import *
from caterpillar.processing.schema import *


# Error and plumbing tests #
def test_frame():
    frame = Frame()
    frame.update("id", 0, ['A sentence.'])
    frame_copy = frame.copy()

    assert frame_copy.id == frame.id
    assert frame_copy.sequence == frame.sequence
    assert frame_copy.sentences == frame.sentences


# Functional tests #
def test_frame_stream_alice():
    """Test frame extraction on Chapter 1 of Alice in Wonderland."""
    with open(os.path.abspath('caterpillar/test_resources/alice_test_data.txt'), 'r') as f:
        frames = frame_stream(f, meta_data={'document': 'alice_test_data.txt'})
        index = 0
        for f in frames:
            if index == 0:
                assert f.metadata['document'] == 'alice_test_data.txt'
            index += 1
        assert index == 49


def test_frame_stream_economics():
    """Test frame extraction on a Wikipedia page about Economics."""
    with open(os.path.abspath('caterpillar/test_resources/economics_test_data.txt'), 'r') as f:
        frames = frame_stream(f, meta_data={'document': 'economics_test_data.txt'})
        index = 0
        for f in frames:
            if index == 0:
                assert f.metadata['document'] == 'economics_test_data.txt'
            index += 1
        assert index == 7


def test_frame_stream_csv_regular():
    """Test CSV frame extraction using some customer feedback data."""
    columns = [ColumnSpec('respondant', ColumnDataType.INTEGER),
               ColumnSpec('region', ColumnDataType.STRING),
               ColumnSpec('store', ColumnDataType.STRING),
               ColumnSpec('liked', ColumnDataType.TEXT),
               ColumnSpec('disliked', ColumnDataType.TEXT),
               ColumnSpec('would_like', ColumnDataType.TEXT),
               ColumnSpec('nps', ColumnDataType.INTEGER)]
    csv_schema = CsvSchema(columns, True, csv.excel)
    with open(os.path.abspath('caterpillar/test_resources/test_small.csv'), 'rbU') as f:
        frames = frame_stream_csv(f, csv_schema, meta_data={'document': 'test_small.csv'})
        index = 0
        for f in frames:
            if index == 0:
                assert f.metadata['document'] == 'test_small.csv'
                assert f.metadata['row_seq'] == '1'
            elif index == 14:
                assert f.metadata['nps'] == '1'
            index += 1
        assert index == 39


def test_frame_stream_csv_with_cr():
    """Test CSV frame extraction using customer feedback data with OSX style newline characters."""
    columns = [ColumnSpec('respondant', ColumnDataType.INTEGER),
               ColumnSpec('region', ColumnDataType.STRING),
               ColumnSpec('store', ColumnDataType.STRING),
               ColumnSpec('liked', ColumnDataType.TEXT),
               ColumnSpec('disliked', ColumnDataType.TEXT),
               ColumnSpec('would_like', ColumnDataType.TEXT),
               ColumnSpec('nps', ColumnDataType.INTEGER)]
    csv_schema = CsvSchema(columns, True, csv.excel)
    with open(os.path.abspath('caterpillar/test_resources/test_with_CR.csv'), 'rbU') as f:
        frames = frame_stream_csv(f, csv_schema, meta_data={'document': 'test_small.csv'})
        index = 0
        for f in frames:
            if index == 0:
                assert f.metadata['document'] == 'test_small.csv'
                assert f.metadata['row_seq'] == '1'
            elif index == 14:
                assert f.metadata['nps'] == '1'
            index += 1
        assert index == 39


def test_frame_stream_csv_cell_as_frame():
    """Text CSV frame extraction with frame size set to 0 (treat each cell as a frame no matter the length)."""
    columns = [ColumnSpec('respondant', ColumnDataType.INTEGER),
               ColumnSpec('region', ColumnDataType.STRING),
               ColumnSpec('store', ColumnDataType.STRING),
               ColumnSpec('liked', ColumnDataType.TEXT),
               ColumnSpec('disliked', ColumnDataType.TEXT),
               ColumnSpec('would_like', ColumnDataType.TEXT),
               ColumnSpec('nps', ColumnDataType.INTEGER)]
    csv_schema = CsvSchema(columns, True, csv.excel)
    with open(os.path.abspath('caterpillar/test_resources/test_small.csv'), 'rbU') as f:
        frames = frame_stream_csv(f, csv_schema, meta_data={'document': 'test_small.csv'}, frame_size=0)
        index = 0
        for f in frames:
            if index == 0:
                assert f.metadata['document'] == 'test_small.csv'
                assert f.metadata['row_seq'] == '1'
            elif index == 14:
                assert f.metadata['nps'] == '1'
            index += 1
        assert index == 35


def test_frame_stream_csv_bad_row():
    """Test CSV frame extraction with row with extraneous cells."""
    columns = [ColumnSpec('Sentiment', ColumnDataType.IGNORE),
               ColumnSpec('Text', ColumnDataType.TEXT)]
    csv_schema = CsvSchema(columns, True, csv.excel)
    with open(os.path.abspath('caterpillar/test_resources/twitter_sentiment.csv'), 'rbU') as f:
        frames = list(frame_stream_csv(f, csv_schema, meta_data={'document': 'twitter_sentiment.csv'}, frame_size=0))

        assert len(frames) == 401
