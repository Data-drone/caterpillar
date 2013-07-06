# caterpillar - Extract frames from a CSV file
#
# Copyright (C) 2012-2013 Mammoth Labs
# Author: Ryan Stuart <ryan@mammothlabs.com.au>
from caterpillar.processing.frames import *

with open('examples/big.csv', 'rbU') as csvfile:
    frames = list(frame_stream_csv(csvfile,
                              [ColumnSpec('respondant', ColumnDataType.INTEGER),
                               ColumnSpec('region', ColumnDataType.STRING),
                               ColumnSpec('store', ColumnDataType.STRING),
                               ColumnSpec('liked', ColumnDataType.TEXT),
                               ColumnSpec('disliked', ColumnDataType.TEXT),
                               ColumnSpec('would_like', ColumnDataType.TEXT),
                               ColumnSpec('nps', ColumnDataType.INTEGER)
                               ]))
