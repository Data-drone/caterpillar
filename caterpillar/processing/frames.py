# caterpillar: Tools to work with text frames
#
# Copyright (C) 2012-2013 Mammoth Labs
# Author: Ryan Stuart <ryan@mammothlabs.com.au>

import uuid
from caterpillar.processing.tokenize import ParagraphTokenizer

import nltk.data


class Frame(object):
    """
    A frame is a piece of text who's size is measured in sentences. A frame has a minimum size of 1. A frame also has
    additional information about itself including a dict of word frequencies, a dict of metadata, a string
    representation of its original form, a unique identifier, a sequence number and optionally a list of unique words.

    """

    def __init__(self, id, sequence, text, metadata=None, unique_words=None):
        """
        Create a new frame from the passed id string, sequence int, text string, frequencies dict and optional metadata
        dict.

        """
        self.id = id
        self.text = text
        self.sequence = sequence
        self.metadata = metadata
        self.unique_words = unique_words


WINDOW_SIZE = 1024*1024*10  # our sliding window of text will be 10MB big


def frame_stream(text_file, frame_size=2, tokenizer=nltk.data.load('tokenizers/punkt/english.pickle'), meta_data=None,
                 encoding='utf-8'):

    """
    This generator function yields text frames parsed from text_file.

    A frame is defined as a block of text who's size is measured in sentences and is at least on sentence long. A frame
    can have meta data associated with it. Some of this meta data is passed to this function directly (original document
    name as text for example or maybe even document author). Other meta data will be emergent from the text frames
    themselves.

    Required arguments:
    text_file -- a file object returned by something like open() where the text data is to be read from.

    Keyword arguments:
    frame_size -- the size of the frames to yield as an int (default 2)
    tokenizer -- an object instance with a tokenize method that accepts a string and returns sentences.

    """
    window = u""
    input = text_file.read(WINDOW_SIZE)
    sequence_number = 1
    while input:
        window += input.decode(encoding)
        paragraphs = ParagraphTokenizer().tokenize(window)
        for paragraph in paragraphs[:-1]:  # Never tokenize the last paragraph in case it isn't complete
            sentences = tokenizer.tokenize(paragraph, realign_boundaries=True)
            frames_text = (" ".join(sentences[i:i+frame_size]) for i in xrange(0, len(sentences), frame_size))
            for text in frames_text:
                yield Frame(uuid.uuid4(), sequence_number, text, meta_data)
                sequence_number += 1
        window = window[window.rfind(paragraphs[-1]):]
        window = window.lstrip()  # Don't want it starting with spaces!
        input = text_file.read(WINDOW_SIZE - len(window))
    paragraphs = ParagraphTokenizer().tokenize(window)
    for paragraph in paragraphs:
        sentences = tokenizer.tokenize(paragraph, realign_boundaries=True)
        frames_text = (" ".join(sentences[i:i+frame_size]) for i in xrange(0, len(sentences), frame_size))
        for text in frames_text:
            yield Frame(uuid.uuid4(), sequence_number, text, meta_data)
            sequence_number += 1