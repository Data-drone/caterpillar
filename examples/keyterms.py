# caterpillar - Extract keyterms from a text file.
#
# Copyright (C) 2012-2013 Mammoth Labs
# Author: Ryan Stuart <ryan@mammothlabs.com.au>
from caterpillar.analytics import keyterms
from caterpillar.processing import index
from caterpillar.processing.frames import frame_stream


with open('examples/moby.txt', 'r') as file:
    frames = frame_stream(file)
    text_index = index.build_text_index(frames)
    keyterms.calculate_influence_values(text_index)
    keyterms.keyterms_by_influence(text_index)