# caterpillar - extract keterms from a csv file
#
# Copyright (C) 2012-2013 Mammoth Labs
# Author: Ryan Stuart <ryan@mammothlabs.com.au>
from caterpillar.analytics import keyterms
from caterpillar.processing import index
from caterpillar.processing.frames import *


with open('examples/big.csv', 'r') as file:
    frames = frame_stream_csv(file,
                              [ColumnSpec('respondant', ColumnDataType.INTEGER),
                               ColumnSpec('region', ColumnDataType.STRING),
                               ColumnSpec('store', ColumnDataType.STRING),
                               ColumnSpec('liked', ColumnDataType.TEXT),
                               ColumnSpec('disliked', ColumnDataType.TEXT),
                               ColumnSpec('would_like', ColumnDataType.TEXT),
                               ColumnSpec('nps', ColumnDataType.INTEGER)
                               ])
    text_index = index.build_text_index(frames)
    keyterms.calculate_influence_values(text_index)
    keyterms.keyterms_by_influence(text_index)