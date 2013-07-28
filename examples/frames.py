# caterpillar - Extract frames from a text file
#
# Copyright (C) 2012-2013 Mammoth Labs
# Author: Ryan Stuart <ryan@mammothlabs.com.au>
from caterpillar.processing.frames import *

with open('examples/moby.txt', 'r') as file:
    # Frames will be a generator
    frames = frame_stream(file)