# caterpillar: Tools to work with and extract text frames
#
# Copyright (C) 2012-2013 Mammoth Labs
# Author: Ryan Stuart <ryan@mammothlabs.com.au>, Kris Rogers <kris@mammothlabs.com.au>
import csv
import logging
from StringIO import StringIO
import uuid

import nltk.data

from caterpillar.processing.analysis.tokenize import ParagraphTokenizer
from caterpillar.processing.schema import ColumnDataType


logger = logging.getLogger(__name__)


class Frame(object):
    """
    A frame is a piece of text who's size is measured in sentences.

    A frame has a minimum size of 1. A frame also has additional information about itself including a dict of word
    frequencies, a dict of metadata, a string representation of its original form, a unique identifier, a sequence
    number and optionally a list of unique words.

    Because object instantiation in Python is slow all frame_stream functions use this class as singleton. This means
    that ONE SINGLE Token object is YIELD OVER AND OVER by frame_stream functions, changing the attributes each time.

    """

    def __init__(self):
        self.id = None
        self.sentences = None
        self.sequence = None
        self.metadata = None
        self.unique_terms = None

    def update(self, id, sequence, sentences, metadata=None, unique_terms=None):
        """
        Reinitialise this frame.

        Required Arguments:
        id -- A string that uniquely identifies this frame.
        sequence -- A int sequence number for this frame.
        sentences -- A list of sentence strings for this frame.
        matedata -- A dict of metadata for this frame
        unique_terms -- A set of unique_terms for this frame.

        """
        self.id = id
        self.sentences = sentences
        self.sequence = sequence
        self.metadata = metadata
        self.unique_terms = unique_terms
        return self

    def copy(self):
        """
        Return a deep copy of this object.

        """
        frame = Frame()
        frame.update(self.id, self.sequence, self.sentences, self.metadata, self.unique_terms)
        return frame


WINDOW_SIZE = 1024*1024*10  # our sliding window of text will be 10MB big


def frame_stream(text_file, frame_size=2, tokenizer=nltk.data.load('tokenizers/punkt/english.pickle'), meta_data=None,
                 encoding='utf-8'):
    """
    This generator function yields text ``Frame``s parsed from text_file.

    A ``Frame`` is defined as a block of text who's size is measured in sentences and is at least on sentence long. It
    can have metadata associated with it. Some of this metadata is passed to this function directly (original document
    name as text for example or maybe even document author). Other metadata could possibly be emergent from the text
    frames themselves.

    Required arguments:
    text_file -- A file object returned by something like open() where the text data is to be read from.

    Keyword arguments:
    frame_size -- The size of the frames to yield as an int (default 2). If this argument is less than 1 then only
                  1 frame is returned containing all the text in the passed file like object.
    tokenizer -- An object instance with a tokenize method that accepts a string and returns sentences.
    meta_data -- A dict of meta data values.
    encoding -- The encoding of the strings read from text_file.

    Returns frame objects. WARNING for performance reasons only 1 instance of Frame is created and it is reused by this
    generator. Do no store references to the returned object! If you really need to store ``Frame`` objects call the
    instance's copy method.

    """
    frame = Frame()  # Only use one instance of frame for performance!
    logger.info('Extracting frames from stream')
    text_file.seek(0)   # Always read from start of the file
    sequence_number = 1
    if frame_size > 0:
        # Break text up into frames of frame_size sentences long
        window = u""
        input = text_file.read(WINDOW_SIZE)
        while input:
            window += input.decode(errors='ignore')
            paragraphs = ParagraphTokenizer().tokenize(window)
            # Because the tokenizers need to be generators for performance, the following code isn't very nice.
            last_paragraph = None
            for paragraph in paragraphs:  # Never tokenize the last paragraph in case it isn't complete
                if last_paragraph:
                    sentences = tokenizer.tokenize(last_paragraph, realign_boundaries=True)
                    # Create a list of sentences per frame, re-init the frame and yield
                    sentences_in_frames = [sentences[i:i+frame_size] for i in xrange(0, len(sentences), frame_size)]
                    for sentence_list in sentences_in_frames:
                        yield frame.update(uuid.uuid4(), sequence_number, sentence_list, meta_data)
                        sequence_number += 1
                last_paragraph = paragraph.value  # Can't store paragraph directly, it is the single Token object!
            window = window[window.rfind(last_paragraph):]
            window = window.lstrip()  # Don't want it starting with spaces!
            input = text_file.read(WINDOW_SIZE - len(window))
        paragraphs = ParagraphTokenizer().tokenize(window)
        for paragraph in paragraphs:
            sentences = tokenizer.tokenize(paragraph.value, realign_boundaries=True)
            sentences_in_frames = [sentences[i:i+frame_size] for i in xrange(0, len(sentences), frame_size)]
            for sentence_list in sentences_in_frames:
                yield frame.update(uuid.uuid4(), sequence_number, sentence_list, meta_data)
                sequence_number += 1
    else:
        # Return all text in 1 frame
        input = text_file.read().decode(encoding)
        sentences = tokenizer.tokenize(input.strip(), realign_boundaries=True)
        yield frame.update(uuid.uuid4(), sequence_number, sentences, meta_data)
    logger.info('Frame extraction complete')


def frame_stream_csv(csv_file, csv_schema, frame_size=2, tokenizer=nltk.data.load('tokenizers/punkt/english.pickle'),
                     meta_data=None, encoding='utf-8', delimiter=','):
    """
    This generator function yields text frames parsed from csv_file.

    A frame is defined as a block of text who's size is measured in sentences and is at least on sentence long. A frame
    can have metadata associated with it. Some of this metadata is passed to this function directly (original document
    name as text for example or maybe even document author). Other metadata could be emergent from the text frames
    themselves. In the case of CSV files, all values in non-textual cells will be captured as metadata.

    To parse a CSV file this function needs a column_spec which is a list of ColumnSpec objects. The order of the
    ColumnSpec objects in the passed list must correspond to the order of columns in the passed csv_file. All columns
    must be accounted for in the passed ColumnSpec list. If you aren't interested in a column, set it to
    ColumnDataType.IGNORE.

    Required arguments:
    csv_file -- A file like object returned by something like open() where the text data is to be read from. If this
                is a file object it must be opened with the 'rbU' flag.
    csv_schema -- A ``schema.CsvSchema`` object that will define how to process the csv data file.

    Keyword arguments:
    frame_size -- The size of the frames to yield from text cells as an int (default 2). If this argument is less than 1
                  then all the text in a cell will be returned as a single frame.
    tokenizer -- An object instance with a tokenize method that accepts a string and returns sentences.
    meta_data -- A dict of meta data values for this file.
    encoding -- The encoding of the strings read from text_file.
    delimiter -- A one-character string used to separate fields. It defaults to ','.

    Returns frame objects. WARNING for performance reasons only 1 instance of Frame is created and it is reused by this
    generator. Do no store references to the returned object! If you really need to store ``Frame`` objects call the
    instance's copy method.

    """
    logger.info('Extracting frames from CSV')
    csv_file.seek(0)   # Always read from start of the file
    csv_reader = csv.reader(csv_file, csv_schema.dialect)

    # Don't parse the header
    if csv_schema.has_header:
        csv_reader.next()

    # Do the actual work. Go through row-by-row then cell-by-cell looking at the data type for each cell. If it is a
    # TEXT cell, then add it to a queue for this row. Otherwise, if it isn't an IGNORE cell, add it to the meta data
    # for this row. Then, return to the queue of TEXT columns and extract teh frames from each passing in the discovered
    # meta data.
    column_spec = csv_schema.columns
    row_seq = 1  # Might be interesting to have, so store it
    num_cols = len(column_spec)
    for row in csv_reader:
        row_meta_data = meta_data.copy() if meta_data else dict()
        row_meta_data['row_seq'] = str(row_seq)
        row_seq += 1
        text_queue = []
        index = 0
        for cell in row:
            if index >= num_cols:
                # Row goes beyond column spec range; don't process any more cells in this row.
                logger.warning('Row {} has cell outside of column spec range at index {}. Skipping remainder of row.'.format(row_seq, index))
                break
            if column_spec[index].type != ColumnDataType.IGNORE and cell:
                if column_spec[index].type == ColumnDataType.TEXT:
                    text_queue.append((column_spec[index].name, cell))
                else:
                    row_meta_data[column_spec[index].name] = cell
            index += 1
        # Process the TEXT cells
        for text_cell in text_queue:
            cell_meta_data = row_meta_data.copy()
            cell_meta_data['column'] = text_cell[0]
            for frame in frame_stream(StringIO(text_cell[1]), frame_size=frame_size, tokenizer=tokenizer,
                                      meta_data=cell_meta_data, encoding=encoding):
                yield frame
    logger.info('Frame extraction complete')