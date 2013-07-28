# caterpillar - Create an index from a file
#
# Copyright (C) 2012-2013 Mammoth Labs
# Author: Ryan Stuart <ryan@mammothlabs.com.au>
from caterpillar.processing import index
from caterpillar.processing.frames import frame_stream

with open('examples/moby.txt', 'r') as file:
    frames = frame_stream(file)
    bi_grams = index.find_bi_gram_words(frames)
    frames = frame_stream(file)  # Because frame_stream is a generator we can only use it once
    text_index = index.build_text_index(frames)